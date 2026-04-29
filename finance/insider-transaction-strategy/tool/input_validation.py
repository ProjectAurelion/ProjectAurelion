#!/usr/bin/env python3
"""Preflight validation helpers for uploaded insider and price datasets."""

from __future__ import annotations

import csv
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional


def snake_case(value: str) -> str:
    value = value.strip()
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"[^a-zA-Z0-9]+", "_", value)
    return value.strip("_").lower()


def first_present(row: dict[str, str], *names: str) -> str:
    for name in names:
        value = row.get(name, "")
        if value.strip():
            return value.strip()
    return ""


def parse_date(raw: str) -> Optional[date]:
    raw = raw.strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_float(raw: str) -> Optional[float]:
    raw = raw.strip()
    if not raw:
        return None
    cleaned = raw.replace("$", "").replace(",", "").replace("%", "")
    if cleaned.lower() in {"na", "nan", "none", "null"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def normalize_ticker(raw: str) -> str:
    return raw.strip().upper()


def _format_date_window(values: list[date]) -> str:
    if not values:
        return ""
    return f"{min(values).isoformat()} to {max(values).isoformat()}"


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{path.name} is missing a header row.")
        headers = [snake_case(header) for header in reader.fieldnames]
        rows = [{snake_case(key): (value or "").strip() for key, value in raw.items()} for raw in reader]
    return headers, rows


def validate_insider_csv(path: Path) -> dict[str, object]:
    headers, rows = _read_csv_rows(path)
    header_set = set(headers)
    errors: list[str] = []
    warnings: list[str] = []

    required_groups = {
        "ticker": {"ticker", "symbol"},
        "owner": {"owner_group_id", "insider_id", "insider_name", "reporting_owner_name"},
        "filing_date": {"filing_date", "filed_at", "date_filed"},
        "total_value": {"total_value", "transaction_value", "value", "dollar_value"},
    }
    missing = [
        name
        for name, options in required_groups.items()
        if not header_set.intersection(options)
    ]
    if missing:
        errors.append(f"Missing insider columns for: {', '.join(missing)}.")

    if not header_set.intersection({"eligible_for_signal", "transaction_code", "transaction_type", "transaction_classification"}):
        warnings.append("No explicit transaction classification columns were found. Signal filtering may be weaker.")

    usable_row_count = 0
    parseable_date_rows = 0
    tickers: set[str] = set()
    issuers: set[str] = set()
    dates: list[date] = []
    for row in rows:
        ticker = normalize_ticker(first_present(row, "ticker", "symbol"))
        owner = first_present(row, "owner_group_id", "insider_id", "insider_name", "reporting_owner_name")
        issuer = first_present(row, "issuer_cik", "issuer_name", "company_name") or ticker
        filing_date = parse_date(first_present(row, "filing_date", "filed_at", "date_filed"))
        total_value = parse_float(first_present(row, "total_value", "transaction_value", "value", "dollar_value"))
        if ticker:
            tickers.add(ticker)
        if issuer:
            issuers.add(issuer)
        if filing_date is not None:
            parseable_date_rows += 1
            dates.append(filing_date)
        if ticker and owner and filing_date is not None and total_value is not None:
            usable_row_count += 1

    if rows and parseable_date_rows == 0:
        errors.append("No parseable filing dates were found in the insider file.")
    if rows and usable_row_count == 0:
        errors.append("No usable insider rows were found after checking ticker, owner, filing date, and total value.")
    if rows and usable_row_count < max(5, int(len(rows) * 0.2)):
        warnings.append("Only a small share of insider rows look usable. The file may be missing key fields.")

    return {
        "file_name": path.name,
        "row_count": len(rows),
        "usable_row_count": usable_row_count,
        "unique_ticker_count": len(tickers),
        "unique_issuer_count": len(issuers),
        "date_window": _format_date_window(dates),
        "headers_detected": sorted(header_set),
        "unique_tickers": sorted(tickers),
        "unique_issuers": sorted(issuers),
        "errors": errors,
        "warnings": warnings,
    }


def validate_price_csv(path: Path, benchmark_ticker: str) -> dict[str, object]:
    headers, rows = _read_csv_rows(path)
    header_set = set(headers)
    errors: list[str] = []
    warnings: list[str] = []

    required_groups = {
        "ticker": {"ticker", "symbol"},
        "date": {"date", "trading_date"},
        "close": {"close", "close_price", "adj_close", "adjusted_close"},
        "volume": {"volume"},
    }
    missing = [
        name
        for name, options in required_groups.items()
        if not header_set.intersection(options)
    ]
    if missing:
        errors.append(f"Missing price columns for: {', '.join(missing)}.")

    benchmark = benchmark_ticker.strip().upper()
    usable_row_count = 0
    adjusted_row_count = 0
    market_cap_row_count = 0
    benchmark_row_count = 0
    tickers: set[str] = set()
    dates: list[date] = []
    for row in rows:
        ticker = normalize_ticker(first_present(row, "ticker", "symbol"))
        trading_date = parse_date(first_present(row, "date", "trading_date"))
        close_value = parse_float(first_present(row, "adj_close", "adjusted_close", "close", "close_price"))
        volume = parse_float(first_present(row, "volume"))
        if ticker:
            tickers.add(ticker)
        if trading_date is not None:
            dates.append(trading_date)
        if ticker == benchmark:
            benchmark_row_count += 1
        if first_present(row, "adj_close", "adjusted_close"):
            adjusted_row_count += 1
        if first_present(row, "market_cap"):
            market_cap_row_count += 1
        if ticker and trading_date is not None and close_value is not None and volume is not None:
            usable_row_count += 1

    if rows and usable_row_count == 0:
        errors.append("No usable price rows were found after checking ticker, date, close, and volume.")
    if benchmark_row_count == 0:
        errors.append(f"Benchmark ticker {benchmark} is missing from the normalized price dataset.")
    if adjusted_row_count == 0:
        warnings.append("Adjusted prices were not found. Split-aware return handling may be weaker.")
    if market_cap_row_count == 0:
        warnings.append("Market-cap values were not found. Size-based filtering and segmentation will be limited.")

    return {
        "file_name": path.name,
        "row_count": len(rows),
        "usable_row_count": usable_row_count,
        "unique_ticker_count": len(tickers),
        "date_window": _format_date_window(dates),
        "benchmark_ticker": benchmark,
        "benchmark_row_count": benchmark_row_count,
        "adjusted_price_coverage_pct": round((adjusted_row_count / len(rows)) * 100, 2) if rows else 0.0,
        "market_cap_coverage_pct": round((market_cap_row_count / len(rows)) * 100, 2) if rows else 0.0,
        "headers_detected": sorted(header_set),
        "unique_tickers": sorted(tickers),
        "errors": errors,
        "warnings": warnings,
    }


def build_preflight_summary(
    *,
    insider_csv: Path,
    prices_csv: Path,
    benchmark_ticker: str,
    insider_input_mode: str,
    price_input_mode: str,
) -> dict[str, object]:
    insider = validate_insider_csv(insider_csv)
    prices = validate_price_csv(prices_csv, benchmark_ticker)

    insider_tickers = set(insider.pop("unique_tickers"))
    insider_issuers = insider.pop("unique_issuers")
    price_tickers = set(prices.pop("unique_tickers"))
    overlap = insider_tickers.intersection(price_tickers)
    overlap_pct = round((len(overlap) / len(insider_tickers)) * 100, 2) if insider_tickers else 0.0

    errors = [*insider["errors"], *prices["errors"]]
    warnings = [*insider["warnings"], *prices["warnings"]]
    if insider_tickers and not overlap:
        errors.append("There is no ticker overlap between the insider file and the normalized price dataset.")
    elif insider_tickers and overlap_pct < 60:
        warnings.append("Ticker overlap between insider and price data is limited. Some events may fail because prices are missing.")

    ready = not errors
    headline = (
        "Input check passed. The uploaded or generated datasets look usable for the study."
        if ready
        else "Input check failed. Fix the dataset issues before relying on the study output."
    )

    return {
        "ready": ready,
        "headline": headline,
        "errors": errors,
        "warnings": warnings,
        "insider": {
            **insider,
            "unique_issuer_count": len(insider_issuers),
            "input_mode": insider_input_mode,
        },
        "prices": {
            **prices,
            "input_mode": price_input_mode,
        },
        "crosscheck": {
            "ticker_overlap_count": len(overlap),
            "ticker_overlap_pct_of_insider": overlap_pct,
        },
    }
