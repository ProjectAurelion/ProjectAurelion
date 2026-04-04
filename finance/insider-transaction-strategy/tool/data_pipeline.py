#!/usr/bin/env python3
"""Download Form 4 insider transactions and daily price data for the event study."""

from __future__ import annotations

import argparse
import csv
import gzip
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

from cache_utils import cache_path, read_or_fetch_bytes, read_or_fetch_text
from form4_normalization import mark_superseded_amendments, parse_form4_rows
from price_loader import fetch_yahoo_history

SEC_ARCHIVES_URL = "https://www.sec.gov/Archives"
FORM_TYPES = {"4", "4/A"}
TOOL_DIR = Path(__file__).resolve().parent
CACHE_ROOT = TOOL_DIR / "cache"


@dataclass(frozen=True)
class FilingEntry:
    cik: str
    company_name: str
    form_type: str
    filing_date: date
    path: str


def parse_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def normalize_ticker(value: str) -> str:
    return value.strip().upper()


def parse_ticker_filter(raw: str) -> set[str]:
    if not raw.strip():
        return set()
    return {normalize_ticker(token) for token in raw.replace(",", " ").split() if token.strip()}


def request_bytes(url: str, user_agent: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = response.read()
        content_encoding = response.headers.get("Content-Encoding", "")
        if "gzip" in content_encoding.lower():
            return gzip.decompress(payload)
        return payload


def iter_quarters(start_date: date, end_date: date) -> Iterable[tuple[int, int]]:
    year = start_date.year
    quarter = ((start_date.month - 1) // 3) + 1
    end_key = (end_date.year, ((end_date.month - 1) // 3) + 1)
    while (year, quarter) <= end_key:
        yield year, quarter
        if quarter == 4:
            year += 1
            quarter = 1
        else:
            quarter += 1


def fetch_master_index(
    year: int,
    quarter: int,
    user_agent: str,
    *,
    cache_root: Path = CACHE_ROOT,
) -> list[FilingEntry]:
    url = f"{SEC_ARCHIVES_URL}/edgar/full-index/{year}/QTR{quarter}/master.gz"
    raw = read_or_fetch_bytes(
        cache_path(cache_root, "sec/master-index", url, ".gz"),
        lambda: request_bytes(url, user_agent),
    )
    text = gzip.decompress(raw).decode("latin-1") if raw[:2] == b"\x1f\x8b" else raw.decode("latin-1")

    lines = text.splitlines()
    start_index = 0
    for index, line in enumerate(lines):
        if line.startswith("-----"):
            start_index = index + 1
            break

    entries: list[FilingEntry] = []
    for line in lines[start_index:]:
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company_name, form_type, filed_at, path = parts
        if form_type not in FORM_TYPES:
            continue
        entries.append(
            FilingEntry(
                cik=cik,
                company_name=company_name,
                form_type=form_type,
                filing_date=parse_date(filed_at),
                path=path,
            )
        )
    return entries


def fetch_filing_transactions(
    filing_entry: FilingEntry,
    user_agent: str,
    ticker_filter: set[str],
    *,
    cache_root: Path = CACHE_ROOT,
) -> list[dict[str, str]]:
    source_url = f"{SEC_ARCHIVES_URL}/{filing_entry.path}"
    filing_text = read_or_fetch_text(
        cache_path(cache_root, "sec/filings", source_url, ".txt"),
        lambda: request_bytes(source_url, user_agent).decode("utf-8", errors="ignore"),
    )
    return parse_form4_rows(
        filing_date=filing_entry.filing_date.isoformat(),
        form_type=filing_entry.form_type,
        source_url=source_url,
        filing_text=filing_text,
        ticker_filter=ticker_filter,
    )


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, ...]] = set()
    deduped: list[dict[str, str]] = []
    for row in rows:
        key = (
            row.get("accession", ""),
            row.get("owner_group_id", ""),
            row.get("transaction_table", ""),
            row.get("transaction_date", ""),
            row.get("transaction_code", ""),
            row.get("security_type", ""),
            row.get("shares", ""),
            row.get("price", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def download_form4_transactions(
    *,
    start_date: date,
    end_date: date,
    output_csv: Path,
    user_agent: str,
    ticker_filter: set[str],
    max_filings: Optional[int] = None,
    pause_seconds: float = 0.2,
    cache_root: Path = CACHE_ROOT,
) -> dict[str, object]:
    filing_entries: list[FilingEntry] = []
    for year, quarter in iter_quarters(start_date, end_date):
        filing_entries.extend(fetch_master_index(year, quarter, user_agent, cache_root=cache_root))

    filing_entries = [entry for entry in filing_entries if start_date <= entry.filing_date <= end_date]
    filing_entries.sort(key=lambda entry: (entry.filing_date, entry.path))
    if max_filings is not None:
        filing_entries = filing_entries[:max_filings]

    rows: list[dict[str, str]] = []
    processed_filings = 0
    failed_filings: list[str] = []

    for entry in filing_entries:
        try:
            rows.extend(fetch_filing_transactions(entry, user_agent, ticker_filter, cache_root=cache_root))
            processed_filings += 1
        except Exception:
            failed_filings.append(entry.path)
        time.sleep(pause_seconds)

    raw_row_count = len(rows)
    rows, superseded_row_count = mark_superseded_amendments(rows)
    rows = dedupe_rows(rows)
    rows.sort(
        key=lambda row: (
            row.get("issuer_cik", ""),
            row.get("ticker", ""),
            row.get("filing_date", ""),
            row.get("owner_group_id", ""),
            row.get("transaction_date", ""),
            row.get("accession", ""),
        )
    )

    fieldnames = [
        "accession",
        "form_type",
        "is_amendment",
        "filing_date",
        "acceptance_datetime",
        "period_of_report",
        "filing_lag_days",
        "transaction_date",
        "ticker",
        "issuer_name",
        "issuer_cik",
        "owner_group_id",
        "owner_group_name",
        "reported_owner_count",
        "is_multi_owner_filing",
        "canonical_role",
        "role_detail",
        "transaction_table",
        "transaction_code",
        "acquired_disposed_code",
        "transaction_classification",
        "eligible_for_signal",
        "security_type",
        "ownership_type",
        "is_direct_ownership",
        "shares",
        "price",
        "total_value",
        "data_quality_flags",
        "source_url",
    ]
    write_csv(output_csv, rows, fieldnames)

    return {
        "output_csv": output_csv,
        "row_count": len(rows),
        "raw_row_count": raw_row_count,
        "superseded_row_count": superseded_row_count,
        "processed_filing_count": processed_filings,
        "failed_filing_count": len(failed_filings),
        "failed_filing_paths": failed_filings[:25],
        "unique_tickers": sorted({row["ticker"] for row in rows if row.get("ticker")}),
        "unique_issuers": sorted({row["issuer_cik"] for row in rows if row.get("issuer_cik")}),
        "cache_dir": str(cache_root),
    }


def download_price_history(
    *,
    tickers: set[str],
    benchmark_ticker: str,
    start_date: date,
    end_date: date,
    output_csv: Path,
    lookback_padding_days: int = 60,
    forward_padding_days: int = 400,
    pause_seconds: float = 0.1,
    cache_root: Path = CACHE_ROOT,
) -> dict[str, object]:
    padded_start = start_date - timedelta(days=lookback_padding_days)
    padded_end = end_date + timedelta(days=forward_padding_days)

    all_rows: list[dict[str, str]] = []
    downloaded: list[str] = []
    missing: list[str] = []
    adjusted_price_row_count = 0
    market_cap_row_count = 0

    for ticker in sorted(set(tickers) | {normalize_ticker(benchmark_ticker)}):
        try:
            rows = fetch_yahoo_history(ticker, padded_start, padded_end, cache_root)
            if not rows:
                missing.append(ticker)
                continue

            kept_any = False
            for row in rows:
                trading_date = parse_date(row.get("date", ""))
                if trading_date < padded_start or trading_date > padded_end:
                    continue
                adjusted_price_row_count += 1 if row.get("adj_close") else 0
                market_cap_row_count += 1 if row.get("market_cap") else 0
                all_rows.append(
                    {
                        "ticker": normalize_ticker(ticker),
                        "date": trading_date.isoformat(),
                        "open": row.get("open", ""),
                        "high": row.get("high", ""),
                        "low": row.get("low", ""),
                        "close": row.get("close", ""),
                        "adj_open": row.get("adj_open", ""),
                        "adj_high": row.get("adj_high", ""),
                        "adj_low": row.get("adj_low", ""),
                        "adj_close": row.get("adj_close", ""),
                        "volume": row.get("volume", ""),
                        "market_cap": row.get("market_cap", ""),
                    }
                )
                kept_any = True
            if kept_any:
                downloaded.append(ticker)
            else:
                missing.append(ticker)
        except urllib.error.HTTPError:
            missing.append(ticker)
        time.sleep(pause_seconds)

    all_rows.sort(key=lambda row: (row["ticker"], row["date"]))
    fieldnames = [
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "volume",
        "market_cap",
    ]
    write_csv(output_csv, all_rows, fieldnames)
    return {
        "output_csv": output_csv,
        "row_count": len(all_rows),
        "downloaded_tickers": downloaded,
        "missing_tickers": missing,
        "adjusted_price_row_count": adjusted_price_row_count,
        "market_cap_row_count": market_cap_row_count,
        "cache_dir": str(cache_root),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, help="Inclusive start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="Inclusive end date in YYYY-MM-DD format.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory where CSVs will be written.")
    parser.add_argument(
        "--user-agent",
        required=True,
        help="SEC-compliant User-Agent string, for example 'Alex Christensen alex@example.com'.",
    )
    parser.add_argument(
        "--tickers",
        default="",
        help="Optional comma-separated ticker filter. Leave blank to pull all Form 4 filings in range.",
    )
    parser.add_argument(
        "--max-filings",
        type=int,
        default=250,
        help="Maximum number of Form 4 filings to fetch. Use a higher value for larger studies.",
    )
    parser.add_argument(
        "--benchmark",
        default="SPY",
        help="Benchmark ticker to include in the price download. Default: SPY.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    ticker_filter = parse_ticker_filter(args.tickers)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    insider_csv = args.output_dir / "insider_transactions.csv"
    prices_csv = args.output_dir / "daily_prices.csv"

    insider_result = download_form4_transactions(
        start_date=start_date,
        end_date=end_date,
        output_csv=insider_csv,
        user_agent=args.user_agent,
        ticker_filter=ticker_filter,
        max_filings=args.max_filings,
    )
    price_result = download_price_history(
        tickers=set(insider_result["unique_tickers"]),
        benchmark_ticker=args.benchmark,
        start_date=start_date,
        end_date=end_date,
        output_csv=prices_csv,
    )

    print("Downloaded raw study inputs")
    print(f"Insider rows: {insider_result['row_count']}")
    print(f"Processed SEC filings: {insider_result['processed_filing_count']}")
    print(f"Failed SEC filings: {insider_result['failed_filing_count']}")
    print(f"Superseded amended rows removed: {insider_result['superseded_row_count']}")
    print(f"Unique tickers: {len(insider_result['unique_tickers'])}")
    print(f"Price rows: {price_result['row_count']}")
    print(f"Adjusted-price rows: {price_result['adjusted_price_row_count']}")
    print(f"Price tickers downloaded: {len(price_result['downloaded_tickers'])}")
    if price_result["missing_tickers"]:
        print(f"Missing price tickers: {', '.join(price_result['missing_tickers'][:20])}")
    print(f"SEC/price cache: {CACHE_ROOT}")
    print(f"Insider CSV: {insider_csv}")
    print(f"Prices CSV: {prices_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
