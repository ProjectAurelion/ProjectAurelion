#!/usr/bin/env python3
"""Build a Stage 1 event-study dataset for clustered insider purchases."""

from __future__ import annotations

import argparse
import csv
import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from cluster_logic import assign_overlap_groups, build_raw_cluster_candidates
from event_alignment import (
    build_aligned_series,
    first_trading_index_on_or_after,
    horizon_total_return,
    next_trading_index_after,
)
from stats_utils import bootstrap_mean_ci, format_number, one_sample_ttest, sample_warning_flags, summarize_numeric

HORIZONS = (21, 63, 126, 252)
OPEN_MARKET_PURCHASE_CODES = {"P"}
ENTRY_TIMINGS = {"next_session_close", "next_session_open"}
EXCLUDED_SECURITY_TERMS = (
    "adr",
    "etf",
    "fund",
    "preferred",
    "warrant",
    "right",
    "unit",
)


@dataclass(frozen=True)
class InsiderTrade:
    accession: str
    issuer_cik: str
    issuer_name: str
    ticker: str
    owner_group_id: str
    owner_group_name: str
    canonical_role: str
    role_detail: str
    filing_date: date
    acceptance_datetime: Optional[datetime]
    announcement_date: date
    timing_ambiguous: bool
    filing_lag_days: Optional[int]
    transaction_date: Optional[date]
    shares: Optional[float]
    price: Optional[float]
    total_value: float
    transaction_code: str
    transaction_type: str
    security_type: str
    ownership_type: str
    is_direct_ownership: bool
    data_quality_flags: tuple[str, ...]


@dataclass(frozen=True)
class PriceBar:
    ticker: str
    trading_date: date
    raw_open: Optional[float]
    raw_high: Optional[float]
    raw_low: Optional[float]
    raw_close: float
    adj_open: Optional[float]
    adj_high: Optional[float]
    adj_low: Optional[float]
    adj_close: float
    volume: float
    market_cap: Optional[float]
    has_adjusted_price: bool


@dataclass(frozen=True)
class QualifiedEvent:
    issuer_cik: str
    issuer_name: str
    ticker: str
    event_date: date
    window_start: date
    window_end: date
    entry_date: date
    entry_timing: str
    timing_source: str
    timing_ambiguous: bool
    entry_index: int
    benchmark_entry_index: int
    entry_raw_price: float
    entry_adj_price: float
    benchmark_entry_date: date
    benchmark_entry_adj_price: float
    distinct_insiders: int
    total_purchase_value: float
    canonical_roles: str
    owner_group_names: str
    avg_daily_dollar_volume_20d: float
    market_cap: Optional[float]
    overlap_group_id: str
    overlap_group_size: int
    adjusted_price_available: bool
    investable_under_capacity: bool
    max_position_size_at_adv_limit: float
    adv_participation_rate: float
    liquidity_bucket: str
    microcap_bucket: str
    data_quality_flags: tuple[str, ...]


def snake_case(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def parse_date(raw: str) -> Optional[date]:
    raw = raw.strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {raw}")


def parse_datetime(raw: str) -> Optional[datetime]:
    raw = raw.strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
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
    return float(cleaned)


def parse_int(raw: str) -> Optional[int]:
    raw = raw.strip()
    if not raw:
        return None
    return int(raw)


def first_present(row: dict[str, str], *names: str) -> str:
    for name in names:
        value = row.get(name, "")
        if value.strip():
            return value.strip()
    return ""


def normalize_ticker(raw: str) -> str:
    return raw.strip().upper()


def normalize_role(raw: str) -> str:
    return re.sub(r"\s+", " ", raw.strip())


def normalize_flags(raw: str) -> tuple[str, ...]:
    if not raw.strip():
        return ()
    seen: list[str] = []
    for part in re.split(r"[;,]", raw):
        value = part.strip()
        if value and value not in seen:
            seen.append(value)
    return tuple(seen)


def join_flags(*groups: tuple[str, ...] | list[str] | str) -> tuple[str, ...]:
    seen: list[str] = []
    for group in groups:
        if isinstance(group, str):
            values = normalize_flags(group)
        else:
            values = tuple(item for item in group if item)
        for value in values:
            if value not in seen:
                seen.append(value)
    return tuple(seen)


def is_open_market_purchase(row: dict[str, str]) -> bool:
    if first_present(row, "eligible_for_signal").lower() in {"yes", "true", "1"}:
        return True

    transaction_code = first_present(row, "transaction_code", "code", "sec_transaction_code").upper()
    if transaction_code:
        return transaction_code in OPEN_MARKET_PURCHASE_CODES

    transaction_type = first_present(row, "transaction_type", "type", "transaction_description").lower()
    if not transaction_type:
        return False

    include_terms = ("open market purchase", "open-market purchase", "purchase", "buy")
    exclude_terms = ("sale", "sell", "grant", "gift", "option", "exercise", "conversion", "automatic", "10b5")
    return any(term in transaction_type for term in include_terms) and not any(
        term in transaction_type for term in exclude_terms
    )


def is_common_stock(row: dict[str, str]) -> bool:
    security_type = first_present(
        row,
        "security_type",
        "instrument_type",
        "asset_type",
        "security_title",
    ).lower()
    if not security_type:
        return True
    return not any(term in security_type for term in EXCLUDED_SECURITY_TERMS)


def load_insider_trades(path: Path) -> list[InsiderTrade]:
    trades: list[InsiderTrade] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Insider transactions CSV is missing a header row.")
        for raw_row in reader:
            row = {snake_case(key): (value or "").strip() for key, value in raw_row.items()}
            if not is_open_market_purchase(row) or not is_common_stock(row):
                continue

            ticker = normalize_ticker(first_present(row, "ticker", "symbol"))
            issuer_cik = first_present(row, "issuer_cik") or f"TICKER:{ticker}"
            owner_group_id = first_present(row, "owner_group_id", "insider_id", "insider_name", "reporting_owner_name")
            filing_date = parse_date(first_present(row, "filing_date", "filed_at", "date_filed"))
            acceptance_dt = parse_datetime(first_present(row, "acceptance_datetime"))
            total_value = parse_float(first_present(row, "total_value", "transaction_value", "value", "dollar_value"))
            if not ticker or not owner_group_id or filing_date is None or total_value is None:
                continue

            announcement_date = acceptance_dt.date() if acceptance_dt is not None else filing_date
            row_flags = list(normalize_flags(first_present(row, "data_quality_flags")))
            if acceptance_dt is None and "missing_acceptance_datetime" not in row_flags:
                row_flags.append("missing_acceptance_datetime")

            trades.append(
                InsiderTrade(
                    accession=first_present(row, "accession", "filing_id", "source_url"),
                    issuer_cik=issuer_cik,
                    issuer_name=first_present(row, "issuer_name", "company_name"),
                    ticker=ticker,
                    owner_group_id=owner_group_id,
                    owner_group_name=first_present(row, "owner_group_name", "insider_name"),
                    canonical_role=normalize_role(first_present(row, "canonical_role", "insider_role", "role", "title")),
                    role_detail=first_present(row, "role_detail"),
                    filing_date=filing_date,
                    acceptance_datetime=acceptance_dt,
                    announcement_date=announcement_date,
                    timing_ambiguous=acceptance_dt is None,
                    filing_lag_days=parse_int(first_present(row, "filing_lag_days")),
                    transaction_date=parse_date(first_present(row, "transaction_date", "trade_date")),
                    shares=parse_float(first_present(row, "shares", "transaction_shares")),
                    price=parse_float(first_present(row, "price", "transaction_price")),
                    total_value=total_value,
                    transaction_code=first_present(row, "transaction_code", "code", "sec_transaction_code").upper(),
                    transaction_type=first_present(
                        row,
                        "transaction_classification",
                        "transaction_type",
                        "type",
                        "transaction_description",
                    ),
                    security_type=first_present(
                        row,
                        "security_type",
                        "instrument_type",
                        "asset_type",
                        "security_title",
                    ),
                    ownership_type=first_present(row, "ownership_type"),
                    is_direct_ownership=first_present(row, "is_direct_ownership").lower() == "yes",
                    data_quality_flags=tuple(row_flags),
                )
            )
    trades.sort(key=lambda trade: (trade.issuer_cik, trade.announcement_date, trade.owner_group_id))
    return trades


def load_price_bars(path: Path) -> dict[str, list[PriceBar]]:
    bars_by_ticker: dict[str, list[PriceBar]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Prices CSV is missing a header row.")
        for raw_row in reader:
            row = {snake_case(key): (value or "").strip() for key, value in raw_row.items()}
            ticker = normalize_ticker(first_present(row, "ticker", "symbol"))
            trading_date = parse_date(first_present(row, "date", "trading_date"))
            raw_close = parse_float(first_present(row, "close", "close_price"))
            adj_close = parse_float(first_present(row, "adj_close", "adjusted_close"))
            volume = parse_float(first_present(row, "volume"))
            if not ticker or trading_date is None or raw_close is None or volume is None:
                continue
            bars_by_ticker.setdefault(ticker, []).append(
                PriceBar(
                    ticker=ticker,
                    trading_date=trading_date,
                    raw_open=parse_float(first_present(row, "open", "open_price")),
                    raw_high=parse_float(first_present(row, "high", "high_price")),
                    raw_low=parse_float(first_present(row, "low", "low_price")),
                    raw_close=raw_close,
                    adj_open=parse_float(first_present(row, "adj_open")),
                    adj_high=parse_float(first_present(row, "adj_high")),
                    adj_low=parse_float(first_present(row, "adj_low")),
                    adj_close=adj_close if adj_close is not None else raw_close,
                    volume=volume,
                    market_cap=parse_float(first_present(row, "market_cap")),
                    has_adjusted_price=adj_close is not None,
                )
            )
    for ticker in bars_by_ticker:
        bars_by_ticker[ticker].sort(key=lambda bar: bar.trading_date)
    return bars_by_ticker


def build_signal_candidates(
    trades: list[InsiderTrade],
    *,
    window_days: int,
    min_distinct_insiders: int,
    min_total_value: float,
    cooldown_days: int,
) -> list[dict[str, object]]:
    raw_candidates = build_raw_cluster_candidates(
        trades,
        window_days=window_days,
        min_distinct_insiders=min_distinct_insiders,
        min_total_value=min_total_value,
    )
    return assign_overlap_groups(raw_candidates, cooldown_days=cooldown_days)


def insider_count_bucket(count: int) -> str:
    if count <= 2:
        return "2 insiders"
    if count == 3:
        return "3 insiders"
    return "4+ insiders"


def purchase_value_bucket(value: float) -> str:
    if value < 250_000:
        return "$100k-$250k"
    if value < 1_000_000:
        return "$250k-$1m"
    return "$1m+"


def size_bucket(market_cap: Optional[float]) -> str:
    if market_cap is None:
        return ""
    if market_cap < 1_000_000_000:
        return "$100m-$1b"
    if market_cap < 5_000_000_000:
        return "$1b-$5b"
    return "$5b+"


def liquidity_bucket(avg_dollar_volume: float) -> str:
    if avg_dollar_volume < 2_500_000:
        return "$1m-$2.5m ADV"
    if avg_dollar_volume < 10_000_000:
        return "$2.5m-$10m ADV"
    return "$10m+ ADV"


def microcap_bucket(market_cap: Optional[float], microcap_cutoff: float) -> str:
    if market_cap is None:
        return "unknown_market_cap"
    if market_cap < microcap_cutoff:
        return "microcap"
    return "non_microcap"


def net_return_after_costs(
    gross_return: float,
    *,
    commission_bps_per_side: float,
    slippage_bps_per_side: float,
) -> float:
    per_side_fraction = (commission_bps_per_side + slippage_bps_per_side) / 10000.0
    return ((1.0 + gross_return) * (1.0 - per_side_fraction) / (1.0 + per_side_fraction)) - 1.0


def horizon_completion_flag(
    *,
    stock_complete: bool,
    benchmark_complete: bool,
) -> str:
    if stock_complete and benchmark_complete:
        return "complete"
    if not stock_complete and benchmark_complete:
        return "potential_delisting_or_missing_price"
    if stock_complete and not benchmark_complete:
        return "benchmark_truncated"
    return "truncated_dataset"


def qualify_events(
    *,
    candidates: list[dict[str, object]],
    bars_by_ticker: dict[str, list[PriceBar]],
    benchmark_ticker: str,
    min_price: float,
    min_daily_dollar_volume: float,
    lookback_days: int,
    min_market_cap: float,
    entry_timing: str,
    assumed_position_size: float,
    max_adv_participation: float,
    microcap_cutoff: float,
) -> tuple[list[QualifiedEvent], list[QualifiedEvent], list[dict[str, str]], dict[str, object]]:
    if entry_timing not in ENTRY_TIMINGS:
        raise ValueError(f"Unsupported entry timing: {entry_timing}")

    aligned = {ticker: build_aligned_series(bars, lookback_days) for ticker, bars in bars_by_ticker.items()}
    benchmark_series = aligned.get(benchmark_ticker)
    if benchmark_series is None:
        raise ValueError(f"Benchmark ticker {benchmark_ticker} is missing from the prices CSV.")

    raw_qualified: list[QualifiedEvent] = []
    primary_qualified: list[QualifiedEvent] = []
    candidate_rows: list[dict[str, str]] = []

    market_cap_available_count = 0
    adjusted_price_available_count = 0
    timing_ambiguous_count = 0
    invalid_price_or_liquidity_count = 0

    for candidate in candidates:
        ticker = str(candidate["ticker"])
        series = aligned.get(ticker)
        rejection_reasons: list[str] = []
        entry_index = None
        benchmark_index = None
        entry_date = ""
        benchmark_entry_date = ""
        entry_raw_price = None
        entry_adj_price = None
        benchmark_entry_adj_price = None
        avg_dollar_volume = None
        market_cap = None
        adjusted_price_available = False
        entry_price_proxy_flag = ""
        investable_under_capacity = False
        max_position_size_at_adv_limit = None
        adv_participation_rate = None
        event_liquidity_bucket = ""
        event_microcap_bucket = "unknown_market_cap"

        if series is None:
            rejection_reasons.append("missing_price_history")
        else:
            event_date = candidate["event_date"]
            entry_index = next_trading_index_after(series, event_date)
            if entry_index is None:
                rejection_reasons.append("no_tradable_session_after_public_disclosure")
            else:
                entry_date = series.dates[entry_index].isoformat()
                benchmark_index = first_trading_index_on_or_after(benchmark_series, series.dates[entry_index])
                if benchmark_index is None:
                    rejection_reasons.append(f"benchmark_{benchmark_ticker}_missing_on_or_after_entry")
                else:
                    benchmark_entry_date = benchmark_series.dates[benchmark_index].isoformat()
                    if entry_timing == "next_session_open" and not math.isnan(benchmark_series.adj_open[benchmark_index]):
                        benchmark_entry_adj_price = float(benchmark_series.adj_open[benchmark_index])
                    else:
                        benchmark_entry_adj_price = float(benchmark_series.adj_close[benchmark_index])

                if entry_timing == "next_session_open" and not math.isnan(series.raw_open[entry_index]):
                    entry_raw_price = float(series.raw_open[entry_index])
                    if not math.isnan(series.adj_open[entry_index]):
                        entry_adj_price = float(series.adj_open[entry_index])
                    else:
                        entry_adj_price = float(series.adj_close[entry_index])
                        entry_price_proxy_flag = "adjusted_open_missing_used_adjusted_close_proxy"
                else:
                    entry_raw_price = float(series.raw_close[entry_index])
                    entry_adj_price = float(series.adj_close[entry_index])
                    if entry_timing == "next_session_open":
                        entry_price_proxy_flag = "open_price_missing_used_close_proxy"
                avg_value = float(series.adv[entry_index])
                avg_dollar_volume = None if math.isnan(avg_value) else avg_value
                market_cap_value = float(series.market_cap[entry_index])
                market_cap = None if math.isnan(market_cap_value) else market_cap_value
                adjusted_price_available = bool(series.adjusted_price_coverage[entry_index] > 0)
                if avg_dollar_volume is not None and avg_dollar_volume > 0:
                    max_position_size_at_adv_limit = avg_dollar_volume * max_adv_participation
                    adv_participation_rate = assumed_position_size / avg_dollar_volume
                    investable_under_capacity = assumed_position_size <= max_position_size_at_adv_limit
                    event_liquidity_bucket = liquidity_bucket(avg_dollar_volume)
                event_microcap_bucket = microcap_bucket(market_cap, microcap_cutoff)

                if entry_raw_price <= min_price:
                    rejection_reasons.append(f"entry_price_lte_{min_price:g}")
                if avg_dollar_volume is None:
                    rejection_reasons.append(f"fewer_than_{lookback_days}_prior_trading_days")
                elif avg_dollar_volume <= min_daily_dollar_volume:
                    rejection_reasons.append(f"avg_daily_dollar_volume_lte_{min_daily_dollar_volume:.0f}")
                if market_cap is not None and market_cap <= min_market_cap:
                    rejection_reasons.append(f"market_cap_lte_{min_market_cap:.0f}")

        if rejection_reasons:
            invalid_price_or_liquidity_count += 1

        if market_cap is not None:
            market_cap_available_count += 1
        if adjusted_price_available:
            adjusted_price_available_count += 1
        if candidate["timing_ambiguous"] == "yes":
            timing_ambiguous_count += 1

        candidate_row = {
            "issuer_cik": str(candidate["issuer_cik"]),
            "ticker": ticker,
            "issuer_name": str(candidate["issuer_name"]),
            "event_date": candidate["event_date"].isoformat(),
            "window_start": candidate["window_start"].isoformat(),
            "window_end": candidate["window_end"].isoformat(),
            "distinct_insiders": str(candidate["distinct_insiders"]),
            "total_purchase_value": format_number(float(candidate["total_purchase_value"])),
            "canonical_roles": str(candidate["canonical_roles"]),
            "owner_group_names": str(candidate["owner_group_names"]),
            "timing_ambiguous": str(candidate["timing_ambiguous"]),
            "timing_source": "filing_date" if candidate["timing_ambiguous"] == "yes" else "acceptance_datetime",
            "entry_timing": entry_timing,
            "overlap_group_id": str(candidate["overlap_group_id"]),
            "overlap_group_size": str(candidate["overlap_group_size"]),
            "is_primary_event": str(candidate["is_primary_event"]),
            "is_strongest_in_overlap_group": str(candidate.get("is_strongest_in_overlap_group", "")),
            "data_quality_flags": str(candidate["data_quality_flags"]),
            "entry_date": entry_date,
            "entry_raw_price": format_number(entry_raw_price),
            "entry_adj_price": format_number(entry_adj_price),
            "benchmark_entry_date": benchmark_entry_date,
            "benchmark_entry_adj_price": format_number(benchmark_entry_adj_price),
            "avg_daily_dollar_volume_20d": format_number(avg_dollar_volume),
            "market_cap": format_number(market_cap),
            "market_cap_filter_active": "yes" if market_cap is not None else "no",
            "adjusted_price_available": "yes" if adjusted_price_available else "no",
            "entry_price_proxy_flag": entry_price_proxy_flag,
            "assumed_position_size": format_number(assumed_position_size),
            "max_position_size_at_adv_limit": format_number(max_position_size_at_adv_limit),
            "adv_participation_rate": format_number(adv_participation_rate),
            "max_adv_participation": format_number(max_adv_participation),
            "investable_under_capacity": "yes" if investable_under_capacity else "no",
            "liquidity_bucket": event_liquidity_bucket,
            "microcap_bucket": event_microcap_bucket,
            "qualified_for_study": "yes" if not rejection_reasons else "no",
            "included_in_primary_study": "yes"
            if not rejection_reasons and candidate["is_primary_event"] == "yes"
            else "no",
            "rejection_reason": "; ".join(rejection_reasons),
        }
        candidate_rows.append(candidate_row)

        if rejection_reasons or series is None or entry_index is None or benchmark_index is None:
            continue

        event = QualifiedEvent(
            issuer_cik=str(candidate["issuer_cik"]),
            issuer_name=str(candidate["issuer_name"]),
            ticker=ticker,
            event_date=candidate["event_date"],
            window_start=candidate["window_start"],
            window_end=candidate["window_end"],
            entry_date=series.dates[entry_index],
            entry_timing=entry_timing,
            timing_source="filing_date" if candidate["timing_ambiguous"] == "yes" else "acceptance_datetime",
            timing_ambiguous=candidate["timing_ambiguous"] == "yes",
            entry_index=entry_index,
            benchmark_entry_index=benchmark_index,
            entry_raw_price=float(entry_raw_price),
            entry_adj_price=float(entry_adj_price),
            benchmark_entry_date=benchmark_series.dates[benchmark_index],
            benchmark_entry_adj_price=float(benchmark_entry_adj_price),
            distinct_insiders=int(candidate["distinct_insiders"]),
            total_purchase_value=float(candidate["total_purchase_value"]),
            canonical_roles=str(candidate["canonical_roles"]),
            owner_group_names=str(candidate["owner_group_names"]),
            avg_daily_dollar_volume_20d=float(avg_dollar_volume or 0.0),
            market_cap=market_cap,
            overlap_group_id=str(candidate["overlap_group_id"]),
            overlap_group_size=int(candidate["overlap_group_size"]),
            adjusted_price_available=adjusted_price_available,
            investable_under_capacity=investable_under_capacity,
            max_position_size_at_adv_limit=float(max_position_size_at_adv_limit or 0.0),
            adv_participation_rate=float(adv_participation_rate or 0.0),
            liquidity_bucket=event_liquidity_bucket,
            microcap_bucket=event_microcap_bucket,
            data_quality_flags=join_flags(str(candidate["data_quality_flags"]), entry_price_proxy_flag),
        )
        raw_qualified.append(event)
        if candidate["is_primary_event"] == "yes":
            primary_qualified.append(event)

    primary_investable_count = sum(1 for event in primary_qualified if event.investable_under_capacity)
    coverage = {
        "raw_candidate_count": len(candidates),
        "raw_qualified_event_count": len(raw_qualified),
        "primary_qualified_event_count": len(primary_qualified),
        "primary_investable_event_count": primary_investable_count,
        "primary_capacity_constrained_event_count": len(primary_qualified) - primary_investable_count,
        "overlap_group_count": len({row["overlap_group_id"] for row in candidate_rows}),
        "secondary_overlap_candidate_count": sum(1 for row in candidate_rows if row["is_primary_event"] == "no"),
        "candidate_market_cap_coverage_pct": (market_cap_available_count / len(candidate_rows) * 100.0)
        if candidate_rows
        else 0.0,
        "candidate_adjusted_price_coverage_pct": (adjusted_price_available_count / len(candidate_rows) * 100.0)
        if candidate_rows
        else 0.0,
        "candidate_timing_ambiguous_pct": (timing_ambiguous_count / len(candidate_rows) * 100.0) if candidate_rows else 0.0,
        "excluded_for_invalid_price_or_liquidity_count": invalid_price_or_liquidity_count,
        "market_cap_filter_active": market_cap_available_count > 0,
        "assumed_position_size": assumed_position_size,
        "max_adv_participation_pct": max_adv_participation * 100.0,
        "primary_investable_pct": (primary_investable_count / len(primary_qualified) * 100.0) if primary_qualified else 0.0,
    }
    return raw_qualified, primary_qualified, candidate_rows, coverage


def build_output_tables(
    *,
    primary_events: list[QualifiedEvent],
    bars_by_ticker: dict[str, list[PriceBar]],
    benchmark_ticker: str,
    lookback_days: int,
    commission_bps_per_side: float,
    slippage_bps_per_side: float,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], dict[str, int]]:
    aligned = {ticker: build_aligned_series(bars, lookback_days) for ticker, bars in bars_by_ticker.items()}
    benchmark_series = aligned[benchmark_ticker]

    event_rows: list[dict[str, str]] = []
    horizon_observations: dict[tuple[str, int], list[dict[str, float]]] = {}
    segment_observations: dict[tuple[str, int, str, str], list[dict[str, float]]] = {}
    horizon_status_counts: dict[str, int] = {
        "complete": 0,
        "potential_delisting_or_missing_price": 0,
        "benchmark_truncated": 0,
        "truncated_dataset": 0,
    }

    for event in primary_events:
        series = aligned[event.ticker]
        row = {
            "issuer_cik": event.issuer_cik,
            "ticker": event.ticker,
            "issuer_name": event.issuer_name,
            "event_date": event.event_date.isoformat(),
            "window_start": event.window_start.isoformat(),
            "window_end": event.window_end.isoformat(),
            "entry_date": event.entry_date.isoformat(),
            "entry_timing": event.entry_timing,
            "timing_source": event.timing_source,
            "timing_ambiguous": "yes" if event.timing_ambiguous else "no",
            "entry_raw_price": format_number(event.entry_raw_price),
            "entry_adj_price": format_number(event.entry_adj_price),
            "benchmark_entry_date": event.benchmark_entry_date.isoformat(),
            "benchmark_entry_adj_price": format_number(event.benchmark_entry_adj_price),
            "distinct_insiders": str(event.distinct_insiders),
            "insider_count_bucket": insider_count_bucket(event.distinct_insiders),
            "total_purchase_value": format_number(event.total_purchase_value),
            "purchase_value_bucket": purchase_value_bucket(event.total_purchase_value),
            "avg_daily_dollar_volume_20d": format_number(event.avg_daily_dollar_volume_20d),
            "market_cap": format_number(event.market_cap),
            "size_bucket": size_bucket(event.market_cap),
            "canonical_roles": event.canonical_roles,
            "owner_group_names": event.owner_group_names,
            "overlap_group_id": event.overlap_group_id,
            "overlap_group_size": str(event.overlap_group_size),
            "adjusted_price_available": "yes" if event.adjusted_price_available else "no",
            "investable_under_capacity": "yes" if event.investable_under_capacity else "no",
            "max_position_size_at_adv_limit": format_number(event.max_position_size_at_adv_limit),
            "adv_participation_rate": format_number(event.adv_participation_rate),
            "liquidity_bucket": event.liquidity_bucket,
            "microcap_bucket": event.microcap_bucket,
            "data_quality_flags": "; ".join(event.data_quality_flags),
        }

        for horizon in HORIZONS:
            stock_return, stock_exit_index, stock_complete = horizon_total_return(
                series,
                event.entry_index,
                horizon,
                entry_price=event.entry_adj_price,
            )
            benchmark_return, benchmark_exit_index, benchmark_complete = horizon_total_return(
                benchmark_series,
                event.benchmark_entry_index,
                horizon,
                entry_price=event.benchmark_entry_adj_price,
            )
            bhar_return = ((1.0 + stock_return) / (1.0 + benchmark_return)) - 1.0
            arithmetic_excess = stock_return - benchmark_return
            net_stock_return = net_return_after_costs(
                stock_return,
                commission_bps_per_side=commission_bps_per_side,
                slippage_bps_per_side=slippage_bps_per_side,
            )
            net_bhar_return = ((1.0 + net_stock_return) / (1.0 + benchmark_return)) - 1.0
            complete = stock_complete and benchmark_complete
            horizon_flag = horizon_completion_flag(stock_complete=stock_complete, benchmark_complete=benchmark_complete)
            horizon_status_counts[horizon_flag] = horizon_status_counts.get(horizon_flag, 0) + 1

            row[f"exit_date_{horizon}d"] = series.dates[stock_exit_index].isoformat()
            row[f"benchmark_exit_date_{horizon}d"] = benchmark_series.dates[benchmark_exit_index].isoformat()
            row[f"raw_return_{horizon}d"] = format_number(stock_return)
            row[f"benchmark_return_{horizon}d"] = format_number(benchmark_return)
            row[f"bhar_return_{horizon}d"] = format_number(bhar_return)
            row[f"net_raw_return_{horizon}d"] = format_number(net_stock_return)
            row[f"net_bhar_return_{horizon}d"] = format_number(net_bhar_return)
            row[f"arithmetic_excess_return_{horizon}d"] = format_number(arithmetic_excess)
            row[f"complete_{horizon}d"] = "yes" if complete else "no"
            row[f"horizon_flag_{horizon}d"] = horizon_flag

            if complete:
                observation = {
                    "raw_return": stock_return,
                    "benchmark_return": benchmark_return,
                    "bhar_return": bhar_return,
                    "net_raw_return": net_stock_return,
                    "net_bhar_return": net_bhar_return,
                    "arithmetic_excess_return": arithmetic_excess,
                }
                horizon_observations.setdefault(("all_primary", horizon), []).append(observation)
                if event.investable_under_capacity:
                    horizon_observations.setdefault(("investable_primary", horizon), []).append(observation)
                for segment_type, segment_name in (
                    ("insider_count_bucket", insider_count_bucket(event.distinct_insiders)),
                    ("purchase_value_bucket", purchase_value_bucket(event.total_purchase_value)),
                    ("size_bucket", size_bucket(event.market_cap)),
                    ("liquidity_bucket", event.liquidity_bucket),
                    ("microcap_bucket", event.microcap_bucket),
                    ("investability", "investable" if event.investable_under_capacity else "capacity_constrained"),
                ):
                    if not segment_name:
                        continue
                    segment_observations.setdefault(("all_primary", horizon, segment_type, segment_name), []).append(observation)
                    if event.investable_under_capacity:
                        segment_observations.setdefault(("investable_primary", horizon, segment_type, segment_name), []).append(observation)

        event_rows.append(row)

    summary_rows: list[dict[str, str]] = []
    for sample_name in ("all_primary", "investable_primary"):
        for horizon in HORIZONS:
            observations = horizon_observations.get((sample_name, horizon), [])
            raw_returns = [item["raw_return"] for item in observations]
            benchmark_returns = [item["benchmark_return"] for item in observations]
            bhar_returns = [item["bhar_return"] for item in observations]
            net_raw_returns = [item["net_raw_return"] for item in observations]
            net_bhar_returns = [item["net_bhar_return"] for item in observations]
            arithmetic_excess = [item["arithmetic_excess_return"] for item in observations]

            raw_summary = summarize_numeric(raw_returns)
            benchmark_summary = summarize_numeric(benchmark_returns)
            bhar_summary = summarize_numeric(bhar_returns)
            net_raw_summary = summarize_numeric(net_raw_returns)
            net_bhar_summary = summarize_numeric(net_bhar_returns)
            t_stat, p_value = one_sample_ttest(bhar_returns)
            ci_low, ci_high = bootstrap_mean_ci(bhar_returns)
            net_t_stat, net_p_value = one_sample_ttest(net_bhar_returns)
            net_ci_low, net_ci_high = bootstrap_mean_ci(net_bhar_returns)
            warning_flags = sample_warning_flags(len(bhar_returns))
            positive_rate = (
                sum(1 for item in bhar_returns if item > 0) / len(bhar_returns)
                if bhar_returns
                else None
            )
            positive_net_rate = (
                sum(1 for item in net_bhar_returns if item > 0) / len(net_bhar_returns)
                if net_bhar_returns
                else None
            )

            summary_rows.append(
                {
                    "sample_name": sample_name,
                    "horizon_days": str(horizon),
                    "primary_event_count": str(len(primary_events)),
                    "complete_event_count": str(len(observations)),
                    "positive_bhar_rate": format_number(positive_rate),
                    "positive_net_bhar_rate": format_number(positive_net_rate),
                    "mean_raw_return": format_number(raw_summary["mean"]),
                    "mean_net_raw_return": format_number(net_raw_summary["mean"]),
                    "mean_benchmark_return": format_number(benchmark_summary["mean"]),
                    "mean_bhar_return": format_number(bhar_summary["mean"]),
                    "median_bhar_return": format_number(bhar_summary["median"]),
                    "mean_net_bhar_return": format_number(net_bhar_summary["mean"]),
                    "median_net_bhar_return": format_number(net_bhar_summary["median"]),
                    "bhar_stddev": format_number(bhar_summary["stddev"]),
                    "bhar_p25": format_number(bhar_summary["p25"]),
                    "bhar_p75": format_number(bhar_summary["p75"]),
                    "mean_arithmetic_excess_return": format_number(
                        summarize_numeric(arithmetic_excess)["mean"] if arithmetic_excess else None
                    ),
                    "t_statistic": format_number(t_stat),
                    "p_value": format_number(p_value),
                    "bootstrap_ci_low": format_number(ci_low),
                    "bootstrap_ci_high": format_number(ci_high),
                    "net_t_statistic": format_number(net_t_stat),
                    "net_p_value": format_number(net_p_value),
                    "net_bootstrap_ci_low": format_number(net_ci_low),
                    "net_bootstrap_ci_high": format_number(net_ci_high),
                    "warning_flags": "; ".join(warning_flags),
                }
            )

    segment_rows: list[dict[str, str]] = []
    for (sample_name, horizon, segment_type, segment_name), observations in sorted(segment_observations.items()):
        bhar_returns = [item["bhar_return"] for item in observations]
        net_bhar_returns = [item["net_bhar_return"] for item in observations]
        summary = summarize_numeric(bhar_returns)
        net_summary = summarize_numeric(net_bhar_returns)
        t_stat, p_value = one_sample_ttest(bhar_returns)
        ci_low, ci_high = bootstrap_mean_ci(bhar_returns)
        net_t_stat, net_p_value = one_sample_ttest(net_bhar_returns)
        net_ci_low, net_ci_high = bootstrap_mean_ci(net_bhar_returns)
        warning_flags = sample_warning_flags(len(bhar_returns))
        positive_rate = sum(1 for item in bhar_returns if item > 0) / len(bhar_returns) if bhar_returns else None
        positive_net_rate = sum(1 for item in net_bhar_returns if item > 0) / len(net_bhar_returns) if net_bhar_returns else None
        segment_rows.append(
            {
                "sample_name": sample_name,
                "horizon_days": str(horizon),
                "segment_type": segment_type,
                "segment_name": segment_name,
                "complete_event_count": str(len(observations)),
                "positive_bhar_rate": format_number(positive_rate),
                "positive_net_bhar_rate": format_number(positive_net_rate),
                "mean_bhar_return": format_number(summary["mean"]),
                "median_bhar_return": format_number(summary["median"]),
                "mean_net_bhar_return": format_number(net_summary["mean"]),
                "median_net_bhar_return": format_number(net_summary["median"]),
                "t_statistic": format_number(t_stat),
                "p_value": format_number(p_value),
                "bootstrap_ci_low": format_number(ci_low),
                "bootstrap_ci_high": format_number(ci_high),
                "net_t_statistic": format_number(net_t_stat),
                "net_p_value": format_number(net_p_value),
                "net_bootstrap_ci_low": format_number(net_ci_low),
                "net_bootstrap_ci_high": format_number(net_ci_high),
                "warning_flags": "; ".join(warning_flags),
            }
        )

    return event_rows, summary_rows, segment_rows, horizon_status_counts


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_warnings(
    *,
    coverage: dict[str, object],
    summary_rows: list[dict[str, str]],
    candidate_rows: list[dict[str, str]],
) -> list[str]:
    warnings: list[str] = []
    if not coverage["market_cap_filter_active"]:
        warnings.append("Market-cap filter is inactive because the price dataset does not contain market-cap values.")
    if float(coverage["candidate_timing_ambiguous_pct"]) > 0:
        warnings.append(
            f"{float(coverage['candidate_timing_ambiguous_pct']):.1f}% of raw cluster candidates rely on filing date because acceptance timestamps were unavailable."
        )
    if float(coverage["candidate_adjusted_price_coverage_pct"]) < 100.0:
        warnings.append("Some candidate events do not have explicit adjusted-price fields and fall back to raw close.")
    if int(coverage["secondary_overlap_candidate_count"]) > 0:
        warnings.append(
            f"{coverage['secondary_overlap_candidate_count']} overlapping raw candidates were excluded from the primary study set to reduce duplicate-event contamination."
        )
    if int(coverage["excluded_for_invalid_price_or_liquidity_count"]) > 0:
        warnings.append(
            f"{coverage['excluded_for_invalid_price_or_liquidity_count']} candidates were excluded for missing sessions, price, liquidity, or benchmark alignment."
        )
    if float(coverage.get("primary_investable_pct", 0.0)) < 70.0 and int(coverage.get("primary_qualified_event_count", 0)) > 0:
        warnings.append(
            f"Only {float(coverage['primary_investable_pct']):.1f}% of primary events meet the assumed capacity limit of {float(coverage['max_adv_participation_pct']):.1f}% ADV."
        )
    if int(coverage.get("potential_delisting_or_missing_count", 0)) > 0:
        warnings.append(
            f"{coverage['potential_delisting_or_missing_count']} event-horizon observations ended early and may reflect delisting or missing price history."
        )
    if float(coverage["candidate_market_cap_coverage_pct"]) == 0.0:
        warnings.append("Microcap separation is unavailable because market-cap coverage is currently zero.")
    elif float(coverage["candidate_market_cap_coverage_pct"]) < 80.0:
        warnings.append(
            f"Microcap separation is only partially available because market-cap coverage is {float(coverage['candidate_market_cap_coverage_pct']):.1f}%."
        )
    if any(row["complete_event_count"] and int(row["complete_event_count"]) < 10 for row in summary_rows):
        warnings.append("Formal inference is suppressed for any horizon with fewer than 10 complete primary events.")
    if any(10 <= int(row["complete_event_count"]) < 30 for row in summary_rows if row["complete_event_count"]):
        warnings.append("Treat horizons with 10-29 complete primary events as exploratory even when statistics are shown.")
    if not candidate_rows:
        warnings.append("No raw insider-buy clusters met the signal definition in the selected window.")
    return warnings


def build_markdown_summary(
    *,
    output_dir: Path,
    benchmark_ticker: str,
    candidate_rows: list[dict[str, str]],
    primary_events: list[QualifiedEvent],
    summary_rows: list[dict[str, str]],
    coverage: dict[str, object],
    warnings: list[str],
    entry_timing: str,
) -> None:
    complete_rows = [
        row for row in summary_rows
        if row["sample_name"] == "all_primary" and row["complete_event_count"] and int(row["complete_event_count"]) > 0
    ]
    investable_rows = [
        row for row in summary_rows
        if row["sample_name"] == "investable_primary" and row["complete_event_count"] and int(row["complete_event_count"]) > 0
    ]
    if not primary_events:
        conclusion = "No primary qualified events passed the current timing, tradability, and data-quality filters."
    elif not complete_rows:
        conclusion = "Primary qualified events exist, but the current price history does not yet support complete forward-return horizons."
    elif any(int(row["complete_event_count"]) < 10 for row in complete_rows):
        conclusion = "The study now controls overlap and timing more carefully, but the current complete-event sample is still too small for formal inference."
    elif sum(float(row["mean_net_bhar_return"] or "0") > 0 for row in investable_rows) >= 2:
        conclusion = "The investable primary-event subset shows positive net benchmark-relative BHAR in multiple horizons and warrants deeper robustness testing."
    else:
        conclusion = "The practical Stage 2 view does not yet show a consistently strong net benchmark-relative signal after simple execution frictions."

    lines = [
        "# Insider Event Study Summary",
        "",
        f"* Benchmark: `{benchmark_ticker}`",
        "* Event-study metric: benchmark-relative BHAR (gross) and net BHAR after simple execution frictions",
        f"* Entry timing: {entry_timing} after public disclosure",
        f"* Raw cluster candidates: {len(candidate_rows)}",
        f"* Primary qualified events: {len(primary_events)}",
        f"* Investable primary events: {coverage['primary_investable_event_count']}",
        f"* Overlap groups: {coverage['overlap_group_count']}",
        f"* Capacity assumption: ${float(coverage['assumed_position_size']):,.0f} per event at <= {float(coverage['max_adv_participation_pct']):.1f}% ADV",
        "",
        "## Conclusion",
        "",
        conclusion,
        "",
        "## Warnings",
        "",
    ]
    if warnings:
        lines.extend(f"* {warning}" for warning in warnings)
    else:
        lines.append("* No Stage 2 warnings were triggered.")
    lines.extend(
        [
            "",
            "## Output Files",
            "",
            "* `signal_candidates.csv`: raw cluster candidates with overlap, timing, and qualification flags",
            "* `qualified_events.csv`: de-overlapped primary events with gross/net BHAR, investability, and horizon-status fields",
            "* `results_summary.csv`: aggregate gross/net BHAR metrics and inference diagnostics by horizon and sample",
            "* `segmented_analysis.csv`: grouped gross/net BHAR metrics by cluster-strength and investability buckets",
        ]
    )
    output_dir.joinpath("summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--insider-csv", required=True, type=Path, help="CSV of insider transactions.")
    parser.add_argument("--prices-csv", required=True, type=Path, help="CSV of daily OHLCV prices.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for generated outputs.")
    parser.add_argument("--benchmark", default="SPY", help="Benchmark ticker symbol. Default: SPY.")
    parser.add_argument("--window-days", type=int, default=30, help="Cluster window in calendar days.")
    parser.add_argument("--cooldown-days", type=int, default=90, help="Cooldown in calendar days per issuer.")
    parser.add_argument("--min-distinct-insiders", type=int, default=2, help="Minimum insiders in the cluster.")
    parser.add_argument("--min-total-value", type=float, default=100000.0, help="Minimum cluster purchase value.")
    parser.add_argument("--min-price", type=float, default=5.0, help="Minimum entry price.")
    parser.add_argument(
        "--min-daily-dollar-volume",
        type=float,
        default=1000000.0,
        help="Minimum trailing average daily dollar volume.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=20,
        help="Trading-day lookback for average daily dollar volume.",
    )
    parser.add_argument(
        "--min-market-cap",
        type=float,
        default=100000000.0,
        help="Minimum market cap when present in the price data.",
    )
    parser.add_argument(
        "--entry-timing",
        choices=sorted(ENTRY_TIMINGS),
        default="next_session_close",
        help="Conservative entry assumption. Default: next_session_close.",
    )
    parser.add_argument(
        "--commission-bps-per-side",
        type=float,
        default=0.0,
        help="Estimated commissions and fees in basis points per side. Default: 0.",
    )
    parser.add_argument(
        "--slippage-bps-per-side",
        type=float,
        default=10.0,
        help="Estimated spread/slippage in basis points per side. Default: 10.",
    )
    parser.add_argument(
        "--assumed-position-size",
        type=float,
        default=50000.0,
        help="Assumed dollars deployed per event for investability diagnostics. Default: 50000.",
    )
    parser.add_argument(
        "--max-adv-participation",
        type=float,
        default=0.1,
        help="Maximum share of 20-day ADV used for the assumed position. Default: 0.10.",
    )
    parser.add_argument(
        "--microcap-cutoff",
        type=float,
        default=300000000.0,
        help="Market-cap cutoff used for microcap separation when market cap is available. Default: 300000000.",
    )
    return parser.parse_args()


def run_study(
    *,
    insider_csv: Path,
    prices_csv: Path,
    output_dir: Path,
    benchmark: str = "SPY",
    window_days: int = 30,
    cooldown_days: int = 90,
    min_distinct_insiders: int = 2,
    min_total_value: float = 100000.0,
    min_price: float = 5.0,
    min_daily_dollar_volume: float = 1000000.0,
    lookback_days: int = 20,
    min_market_cap: float = 100000000.0,
    entry_timing: str = "next_session_close",
    commission_bps_per_side: float = 0.0,
    slippage_bps_per_side: float = 10.0,
    assumed_position_size: float = 50000.0,
    max_adv_participation: float = 0.1,
    microcap_cutoff: float = 300000000.0,
) -> dict[str, object]:
    trades = load_insider_trades(insider_csv)
    if not trades:
        raise ValueError("No eligible insider trades were found after Stage 1 transaction filtering.")

    bars_by_ticker = load_price_bars(prices_csv)
    if not bars_by_ticker:
        raise ValueError("No price bars were loaded from the prices CSV.")

    benchmark_ticker = normalize_ticker(benchmark)
    candidates = build_signal_candidates(
        trades,
        window_days=window_days,
        min_distinct_insiders=min_distinct_insiders,
        min_total_value=min_total_value,
        cooldown_days=cooldown_days,
    )

    raw_qualified, primary_qualified, candidate_rows, coverage = qualify_events(
        candidates=candidates,
        bars_by_ticker=bars_by_ticker,
        benchmark_ticker=benchmark_ticker,
        min_price=min_price,
        min_daily_dollar_volume=min_daily_dollar_volume,
        lookback_days=lookback_days,
        min_market_cap=min_market_cap,
        entry_timing=entry_timing,
        assumed_position_size=assumed_position_size,
        max_adv_participation=max_adv_participation,
        microcap_cutoff=microcap_cutoff,
    )

    event_rows, summary_rows, segment_rows, horizon_status_counts = build_output_tables(
        primary_events=primary_qualified,
        bars_by_ticker=bars_by_ticker,
        benchmark_ticker=benchmark_ticker,
        lookback_days=lookback_days,
        commission_bps_per_side=commission_bps_per_side,
        slippage_bps_per_side=slippage_bps_per_side,
    )
    coverage.update(
        {
            "potential_delisting_or_missing_count": horizon_status_counts.get("potential_delisting_or_missing_price", 0),
            "benchmark_truncated_count": horizon_status_counts.get("benchmark_truncated", 0),
            "truncated_dataset_count": horizon_status_counts.get("truncated_dataset", 0),
            "complete_event_horizon_count": horizon_status_counts.get("complete", 0),
        }
    )
    warnings = build_warnings(coverage=coverage, summary_rows=summary_rows, candidate_rows=candidate_rows)
    methodology = {
        "event_study_framework": "BHAR",
        "benchmark": benchmark_ticker,
        "entry_timing": entry_timing,
        "same_day_execution": "disabled",
        "primary_sample": "de-overlapped primary events only",
        "primary_selection_rule": "earliest candidate per cooldown group to avoid future-informed event selection",
        "inference_policy": "Formal inference suppressed for n < 10 and treated as exploratory for 10 <= n < 30.",
        "commission_bps_per_side": commission_bps_per_side,
        "slippage_bps_per_side": slippage_bps_per_side,
        "assumed_position_size": assumed_position_size,
        "max_adv_participation_pct": max_adv_participation * 100.0,
        "note": "Net returns are event-level realism adjustments, not a portfolio simulation.",
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "signal_candidates.csv", candidate_rows)
    write_csv(output_dir / "qualified_events.csv", event_rows)
    write_csv(output_dir / "results_summary.csv", summary_rows)
    write_csv(output_dir / "segmented_analysis.csv", segment_rows)
    build_markdown_summary(
        output_dir=output_dir,
        benchmark_ticker=benchmark_ticker,
        candidate_rows=candidate_rows,
        primary_events=primary_qualified,
        summary_rows=summary_rows,
        coverage=coverage,
        warnings=warnings,
        entry_timing=entry_timing,
    )

    return {
        "candidate_rows": candidate_rows,
        "raw_qualified_events": raw_qualified,
        "primary_qualified_events": primary_qualified,
        "event_rows": event_rows,
        "summary_rows": summary_rows,
        "segment_rows": segment_rows,
        "candidate_count": len(candidate_rows),
        "qualified_raw_count": len(raw_qualified),
        "qualified_count": len(primary_qualified),
        "rejected_count": len(candidate_rows) - len(raw_qualified),
        "output_dir": output_dir,
        "benchmark": benchmark_ticker,
        "warnings": warnings,
        "coverage": coverage,
        "methodology": methodology,
    }


def build_console_summary(result: dict[str, object]) -> str:
    lines = [
        "Insider Event Study Complete",
        f"Output directory: {result['output_dir']}",
        f"Benchmark: {result['benchmark']}",
        f"Raw cluster candidates: {result['candidate_count']}",
        f"Qualified raw events: {result['qualified_raw_count']}",
        f"Primary qualified events: {result['qualified_count']}",
        f"Investable primary events: {result['coverage']['primary_investable_event_count']}",
        f"Rejected raw candidates: {result['rejected_count']}",
        f"Overlap groups: {result['coverage']['overlap_group_count']}",
        "",
        "By Horizon (primary events, gross/net BHAR)",
    ]

    for row in result["summary_rows"]:
        if row["sample_name"] != "all_primary":
            continue
        lines.append(
            "  "
            + f"{row['horizon_days']}d"
            + f" | complete={row['complete_event_count'] or '0'}"
            + f" | mean_bhar={row['mean_bhar_return'] or 'n/a'}"
            + f" | mean_net_bhar={row['mean_net_bhar_return'] or 'n/a'}"
            + f" | p_value={row['p_value'] or 'n/a'}"
            + f" | warnings={row['warning_flags'] or 'none'}"
        )
    if result["warnings"]:
        lines.extend(["", "Warnings"] + [f"  - {warning}" for warning in result["warnings"]])
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    result = run_study(
        insider_csv=args.insider_csv,
        prices_csv=args.prices_csv,
        output_dir=args.output_dir,
        benchmark=args.benchmark,
        window_days=args.window_days,
        cooldown_days=args.cooldown_days,
        min_distinct_insiders=args.min_distinct_insiders,
        min_total_value=args.min_total_value,
        min_price=args.min_price,
        min_daily_dollar_volume=args.min_daily_dollar_volume,
        lookback_days=args.lookback_days,
        min_market_cap=args.min_market_cap,
        entry_timing=args.entry_timing,
        commission_bps_per_side=args.commission_bps_per_side,
        slippage_bps_per_side=args.slippage_bps_per_side,
        assumed_position_size=args.assumed_position_size,
        max_adv_participation=args.max_adv_participation,
        microcap_cutoff=args.microcap_cutoff,
    )
    print(build_console_summary(result))
    print(f"\nWrote study outputs to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
