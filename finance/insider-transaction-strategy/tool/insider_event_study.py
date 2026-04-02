#!/usr/bin/env python3
"""Build a V1 event-study dataset for clustered insider purchases."""

from __future__ import annotations

import argparse
import bisect
import csv
import math
import re
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Deque, Iterable, Optional

HORIZONS = (21, 63, 126, 252)
OPEN_MARKET_PURCHASE_CODES = {"P"}
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
    ticker: str
    insider_id: str
    insider_role: str
    transaction_date: Optional[date]
    filing_date: date
    shares: Optional[float]
    price: Optional[float]
    total_value: float
    transaction_code: str
    transaction_type: str
    security_type: str


@dataclass(frozen=True)
class PriceBar:
    ticker: str
    trading_date: date
    open_price: Optional[float]
    high_price: Optional[float]
    low_price: Optional[float]
    close_price: float
    volume: float
    market_cap: Optional[float]


@dataclass(frozen=True)
class SignalCandidate:
    ticker: str
    event_date: date
    window_start: date
    window_end: date
    distinct_insiders: int
    total_purchase_value: float
    insider_roles: str


@dataclass(frozen=True)
class QualifiedEvent:
    ticker: str
    event_date: date
    entry_date: date
    entry_price: float
    benchmark_entry_date: date
    benchmark_entry_price: float
    distinct_insiders: int
    total_purchase_value: float
    insider_roles: str
    avg_daily_dollar_volume_20d: float
    market_cap: Optional[float]


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


def parse_float(raw: str) -> Optional[float]:
    raw = raw.strip()
    if not raw:
        return None
    cleaned = raw.replace("$", "").replace(",", "").replace("%", "")
    if cleaned.lower() in {"na", "nan", "none", "null"}:
        return None
    return float(cleaned)


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


def percentile(values: list[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * pct
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def format_number(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.6f}"


def is_open_market_purchase(row: dict[str, str]) -> bool:
    transaction_code = first_present(
        row,
        "transaction_code",
        "code",
        "sec_transaction_code",
    ).upper()
    if transaction_code:
        return transaction_code in OPEN_MARKET_PURCHASE_CODES

    transaction_type = first_present(
        row,
        "transaction_type",
        "type",
        "transaction_description",
    ).lower()
    if not transaction_type:
        return False

    include_terms = (
        "open market purchase",
        "open-market purchase",
        "purchase",
        "buy",
    )
    exclude_terms = (
        "sale",
        "sell",
        "grant",
        "gift",
        "option",
        "exercise",
        "conversion",
        "automatic",
        "10b5",
        "derivative",
    )
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
            insider_id = first_present(row, "insider_id", "insider_name", "reporting_owner_name")
            filing_date = parse_date(first_present(row, "filing_date", "filed_at", "date_filed"))
            total_value = parse_float(
                first_present(row, "total_value", "transaction_value", "value", "dollar_value")
            )
            close_price = parse_float(first_present(row, "price", "transaction_price"))
            shares = parse_float(first_present(row, "shares", "transaction_shares"))
            transaction_date = parse_date(first_present(row, "transaction_date", "trade_date"))

            if not ticker or not insider_id or filing_date is None or total_value is None:
                continue

            trades.append(
                InsiderTrade(
                    ticker=ticker,
                    insider_id=insider_id,
                    insider_role=normalize_role(first_present(row, "insider_role", "role", "title")),
                    transaction_date=transaction_date,
                    filing_date=filing_date,
                    shares=shares,
                    price=close_price,
                    total_value=total_value,
                    transaction_code=first_present(row, "transaction_code", "code", "sec_transaction_code").upper(),
                    transaction_type=first_present(row, "transaction_type", "type", "transaction_description"),
                    security_type=first_present(
                        row,
                        "security_type",
                        "instrument_type",
                        "asset_type",
                        "security_title",
                    ),
                )
            )
    trades.sort(key=lambda trade: (trade.ticker, trade.filing_date, trade.insider_id))
    return trades


def load_price_bars(path: Path) -> dict[str, list[PriceBar]]:
    bars_by_ticker: dict[str, list[PriceBar]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Prices CSV is missing a header row.")
        for raw_row in reader:
            row = {snake_case(key): (value or "").strip() for key, value in raw_row.items()}
            ticker = normalize_ticker(first_present(row, "ticker", "symbol"))
            trading_date = parse_date(first_present(row, "date", "trading_date"))
            close_price = parse_float(first_present(row, "close", "close_price", "adj_close"))
            volume = parse_float(first_present(row, "volume"))
            if not ticker or trading_date is None or close_price is None or volume is None:
                continue
            bars_by_ticker[ticker].append(
                PriceBar(
                    ticker=ticker,
                    trading_date=trading_date,
                    open_price=parse_float(first_present(row, "open", "open_price")),
                    high_price=parse_float(first_present(row, "high", "high_price")),
                    low_price=parse_float(first_present(row, "low", "low_price")),
                    close_price=close_price,
                    volume=volume,
                    market_cap=parse_float(first_present(row, "market_cap")),
                )
            )
    for ticker in bars_by_ticker:
        bars_by_ticker[ticker].sort(key=lambda bar: bar.trading_date)
    return bars_by_ticker


def build_signal_candidates(
    trades: list[InsiderTrade],
    window_days: int,
    min_distinct_insiders: int,
    min_total_value: float,
    cooldown_days: int,
) -> list[SignalCandidate]:
    grouped: dict[str, list[InsiderTrade]] = defaultdict(list)
    for trade in trades:
        grouped[trade.ticker].append(trade)

    candidates: list[SignalCandidate] = []
    for ticker, ticker_trades in grouped.items():
        ticker_trades.sort(key=lambda trade: (trade.filing_date, trade.insider_id))
        window: Deque[InsiderTrade] = deque()
        last_signal_date: Optional[date] = None

        for trade in ticker_trades:
            if last_signal_date is not None and trade.filing_date <= last_signal_date + timedelta(days=cooldown_days):
                continue

            window.append(trade)
            oldest_allowed = trade.filing_date - timedelta(days=window_days)
            while window and window[0].filing_date < oldest_allowed:
                window.popleft()

            distinct_insiders = {item.insider_id for item in window}
            total_value = sum(item.total_value for item in window)
            if len(distinct_insiders) >= min_distinct_insiders and total_value >= min_total_value:
                roles = sorted({item.insider_role for item in window if item.insider_role})
                candidates.append(
                    SignalCandidate(
                        ticker=ticker,
                        event_date=trade.filing_date,
                        window_start=min(item.filing_date for item in window),
                        window_end=max(item.filing_date for item in window),
                        distinct_insiders=len(distinct_insiders),
                        total_purchase_value=total_value,
                        insider_roles="; ".join(roles),
                    )
                )
                last_signal_date = trade.filing_date
                window.clear()
    return candidates


def dates_for_bars(bars: list[PriceBar]) -> list[date]:
    return [bar.trading_date for bar in bars]


def find_first_bar_after(bars: list[PriceBar], target_date: date) -> Optional[int]:
    trading_dates = dates_for_bars(bars)
    index = bisect.bisect_right(trading_dates, target_date)
    if index >= len(bars):
        return None
    return index


def find_first_bar_on_or_after(bars: list[PriceBar], target_date: date) -> Optional[int]:
    trading_dates = dates_for_bars(bars)
    index = bisect.bisect_left(trading_dates, target_date)
    if index >= len(bars):
        return None
    return index


def average_daily_dollar_volume(bars: list[PriceBar], entry_index: int, lookback_days: int) -> Optional[float]:
    if entry_index < lookback_days:
        return None
    lookback = bars[entry_index - lookback_days : entry_index]
    values = [bar.close_price * bar.volume for bar in lookback]
    return statistics.fmean(values) if values else None


def qualify_events(
    candidates: list[SignalCandidate],
    bars_by_ticker: dict[str, list[PriceBar]],
    benchmark_ticker: str,
    min_price: float,
    min_daily_dollar_volume: float,
    lookback_days: int,
    min_market_cap: float,
) -> tuple[list[QualifiedEvent], list[dict[str, str]]]:
    qualified: list[QualifiedEvent] = []
    candidate_rows: list[dict[str, str]] = []
    benchmark_bars = bars_by_ticker.get(benchmark_ticker)
    if not benchmark_bars:
        raise ValueError(f"Benchmark ticker {benchmark_ticker} is missing from the prices CSV.")

    for candidate in candidates:
        ticker_bars = bars_by_ticker.get(candidate.ticker, [])
        rejection_reason = ""
        entry_index = find_first_bar_after(ticker_bars, candidate.event_date) if ticker_bars else None
        benchmark_index = None
        entry_bar = None
        benchmark_bar = None
        avg_dollar_volume = None

        if entry_index is None:
            rejection_reason = "no tradable session after filing date"
        else:
            entry_bar = ticker_bars[entry_index]
            avg_dollar_volume = average_daily_dollar_volume(ticker_bars, entry_index, lookback_days)
            benchmark_index = find_first_bar_on_or_after(benchmark_bars, entry_bar.trading_date)
            benchmark_bar = benchmark_bars[benchmark_index] if benchmark_index is not None else None

            if entry_bar.close_price <= min_price:
                rejection_reason = f"entry price <= {min_price}"
            elif avg_dollar_volume is None:
                rejection_reason = f"fewer than {lookback_days} prior trading days"
            elif avg_dollar_volume <= min_daily_dollar_volume:
                rejection_reason = f"20d avg daily dollar volume <= {min_daily_dollar_volume:.0f}"
            elif entry_bar.market_cap is not None and entry_bar.market_cap <= min_market_cap:
                rejection_reason = f"market cap <= {min_market_cap:.0f}"
            elif benchmark_bar is None:
                rejection_reason = f"benchmark {benchmark_ticker} missing on or after entry date"

        candidate_rows.append(
            {
                "ticker": candidate.ticker,
                "event_date": candidate.event_date.isoformat(),
                "window_start": candidate.window_start.isoformat(),
                "window_end": candidate.window_end.isoformat(),
                "distinct_insiders": str(candidate.distinct_insiders),
                "total_purchase_value": format_number(candidate.total_purchase_value),
                "insider_roles": candidate.insider_roles,
                "entry_date": entry_bar.trading_date.isoformat() if entry_bar else "",
                "entry_price": format_number(entry_bar.close_price if entry_bar else None),
                "avg_daily_dollar_volume_20d": format_number(avg_dollar_volume),
                "market_cap": format_number(entry_bar.market_cap if entry_bar else None),
                "qualified_for_study": "yes" if not rejection_reason else "no",
                "rejection_reason": rejection_reason,
            }
        )

        if rejection_reason or entry_bar is None or benchmark_bar is None:
            continue

        qualified.append(
            QualifiedEvent(
                ticker=candidate.ticker,
                event_date=candidate.event_date,
                entry_date=entry_bar.trading_date,
                entry_price=entry_bar.close_price,
                benchmark_entry_date=benchmark_bar.trading_date,
                benchmark_entry_price=benchmark_bar.close_price,
                distinct_insiders=candidate.distinct_insiders,
                total_purchase_value=candidate.total_purchase_value,
                insider_roles=candidate.insider_roles,
                avg_daily_dollar_volume_20d=avg_dollar_volume or 0.0,
                market_cap=entry_bar.market_cap,
            )
        )

    return qualified, candidate_rows


def forward_return(bars: list[PriceBar], entry_index: int, horizon: int) -> dict[str, object]:
    entry_bar = bars[entry_index]
    exit_index = entry_index + horizon
    if exit_index < len(bars):
        exit_bar = bars[exit_index]
        complete = True
    else:
        exit_bar = bars[-1]
        complete = False

    return {
        "exit_date": exit_bar.trading_date,
        "return": (exit_bar.close_price / entry_bar.close_price) - 1.0,
        "complete": complete,
    }


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


def summarize_numeric(values: list[float]) -> dict[str, Optional[float]]:
    if not values:
        return {
            "mean": None,
            "median": None,
            "stddev": None,
            "p25": None,
            "p75": None,
        }
    return {
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "stddev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "p25": percentile(values, 0.25),
        "p75": percentile(values, 0.75),
    }


def build_output_tables(
    qualified_events: list[QualifiedEvent],
    bars_by_ticker: dict[str, list[PriceBar]],
    benchmark_ticker: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    benchmark_bars = bars_by_ticker[benchmark_ticker]
    event_rows: list[dict[str, str]] = []
    horizon_observations: dict[int, list[dict[str, object]]] = defaultdict(list)
    segment_observations: dict[tuple[int, str, str], list[dict[str, object]]] = defaultdict(list)

    benchmark_date_index = dates_for_bars(benchmark_bars)

    for event in qualified_events:
        ticker_bars = bars_by_ticker[event.ticker]
        ticker_dates = dates_for_bars(ticker_bars)
        entry_index = ticker_dates.index(event.entry_date)
        benchmark_index = benchmark_date_index.index(event.benchmark_entry_date)

        row = {
            "ticker": event.ticker,
            "event_date": event.event_date.isoformat(),
            "entry_date": event.entry_date.isoformat(),
            "entry_price": format_number(event.entry_price),
            "benchmark_entry_date": event.benchmark_entry_date.isoformat(),
            "benchmark_entry_price": format_number(event.benchmark_entry_price),
            "distinct_insiders": str(event.distinct_insiders),
            "insider_count_bucket": insider_count_bucket(event.distinct_insiders),
            "total_purchase_value": format_number(event.total_purchase_value),
            "purchase_value_bucket": purchase_value_bucket(event.total_purchase_value),
            "avg_daily_dollar_volume_20d": format_number(event.avg_daily_dollar_volume_20d),
            "market_cap": format_number(event.market_cap),
            "size_bucket": size_bucket(event.market_cap),
            "insider_roles": event.insider_roles,
        }

        for horizon in HORIZONS:
            stock_return = forward_return(ticker_bars, entry_index, horizon)
            benchmark_return = forward_return(benchmark_bars, benchmark_index, horizon)
            raw_return = stock_return["return"]
            benchmark_value = benchmark_return["return"]
            excess_return = raw_return - benchmark_value

            row[f"exit_date_{horizon}d"] = stock_return["exit_date"].isoformat()
            row[f"raw_return_{horizon}d"] = format_number(raw_return)
            row[f"benchmark_return_{horizon}d"] = format_number(benchmark_value)
            row[f"excess_return_{horizon}d"] = format_number(excess_return)
            row[f"complete_{horizon}d"] = "yes" if stock_return["complete"] and benchmark_return["complete"] else "no"

            if stock_return["complete"] and benchmark_return["complete"]:
                observation = {
                    "raw_return": raw_return,
                    "benchmark_return": benchmark_value,
                    "excess_return": excess_return,
                }
                horizon_observations[horizon].append(observation)
                segment_observations[(horizon, "insider_count_bucket", insider_count_bucket(event.distinct_insiders))].append(observation)
                segment_observations[(horizon, "purchase_value_bucket", purchase_value_bucket(event.total_purchase_value))].append(observation)
                size = size_bucket(event.market_cap)
                if size:
                    segment_observations[(horizon, "size_bucket", size)].append(observation)

        event_rows.append(row)

    summary_rows: list[dict[str, str]] = []
    for horizon in HORIZONS:
        observations = horizon_observations[horizon]
        raw_returns = [item["raw_return"] for item in observations]
        excess_returns = [item["excess_return"] for item in observations]
        raw_summary = summarize_numeric(raw_returns)
        excess_summary = summarize_numeric(excess_returns)
        hit_rate = sum(1 for item in raw_returns if item > 0) / len(raw_returns) if raw_returns else None

        summary_rows.append(
            {
                "horizon_days": str(horizon),
                "qualified_event_count": str(len(qualified_events)),
                "complete_event_count": str(len(observations)),
                "hit_rate": format_number(hit_rate),
                "mean_return": format_number(raw_summary["mean"]),
                "median_return": format_number(raw_summary["median"]),
                "return_stddev": format_number(raw_summary["stddev"]),
                "return_p25": format_number(raw_summary["p25"]),
                "return_p75": format_number(raw_summary["p75"]),
                "mean_excess_return": format_number(excess_summary["mean"]),
                "median_excess_return": format_number(excess_summary["median"]),
                "excess_return_stddev": format_number(excess_summary["stddev"]),
                "excess_return_p25": format_number(excess_summary["p25"]),
                "excess_return_p75": format_number(excess_summary["p75"]),
            }
        )

    segment_rows: list[dict[str, str]] = []
    for (horizon, segment_type, segment_name), observations in sorted(segment_observations.items()):
        raw_returns = [item["raw_return"] for item in observations]
        excess_returns = [item["excess_return"] for item in observations]
        raw_summary = summarize_numeric(raw_returns)
        excess_summary = summarize_numeric(excess_returns)
        hit_rate = sum(1 for item in raw_returns if item > 0) / len(raw_returns) if raw_returns else None

        segment_rows.append(
            {
                "horizon_days": str(horizon),
                "segment_type": segment_type,
                "segment_name": segment_name,
                "complete_event_count": str(len(observations)),
                "hit_rate": format_number(hit_rate),
                "mean_return": format_number(raw_summary["mean"]),
                "median_return": format_number(raw_summary["median"]),
                "mean_excess_return": format_number(excess_summary["mean"]),
                "median_excess_return": format_number(excess_summary["median"]),
            }
        )

    return event_rows, summary_rows, segment_rows


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


def build_markdown_summary(
    output_dir: Path,
    candidate_rows: list[dict[str, str]],
    qualified_events: list[QualifiedEvent],
    summary_rows: list[dict[str, str]],
    benchmark_ticker: str,
) -> None:
    qualified_count = len(qualified_events)
    candidate_count = len(candidate_rows)
    rejected_count = candidate_count - qualified_count

    positive_horizons = 0
    complete_horizons = 0
    for row in summary_rows:
        complete_count = int(row["complete_event_count"] or "0")
        if complete_count > 0:
            complete_horizons += 1
            mean_excess = float(row["mean_excess_return"]) if row["mean_excess_return"] else 0.0
            median_excess = float(row["median_excess_return"]) if row["median_excess_return"] else 0.0
            if mean_excess > 0 and median_excess > 0:
                positive_horizons += 1

    if qualified_count == 0:
        conclusion = "No qualified events passed all filters, so the signal is currently untested."
    elif complete_horizons == 0:
        conclusion = "Qualified events were found, but the current price history does not support complete forward-return horizons yet."
    elif qualified_count < 20:
        conclusion = "The tool produced an initial event set, but the sample is small enough that any signal conclusion should be treated as provisional."
    elif positive_horizons >= 2:
        conclusion = "The initial pass looks promising enough to justify deeper validation and, if repeated on cleaner data, eventual portfolio backtesting."
    else:
        conclusion = "This first pass does not yet show a consistently strong benchmark-relative signal."

    lines = [
        "# Insider Event Study Summary",
        "",
        f"* Benchmark: `{benchmark_ticker}`",
        f"* Signal candidates found: {candidate_count}",
        f"* Qualified events after tradability filters: {qualified_count}",
        f"* Rejected candidates: {rejected_count}",
        "",
        "## Conclusion",
        "",
        conclusion,
        "",
        "## Output Files",
        "",
        "* `signal_candidates.csv`: all detected cluster events and rejection reasons",
        "* `qualified_events.csv`: qualified events with forward-return columns",
        "* `results_summary.csv`: aggregate metrics by horizon",
        "* `segmented_analysis.csv`: grouped results by signal-strength bucket",
    ]
    output_dir.joinpath("summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--insider-csv", required=True, type=Path, help="CSV of insider transactions.")
    parser.add_argument("--prices-csv", required=True, type=Path, help="CSV of daily OHLCV prices.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for generated outputs.")
    parser.add_argument("--benchmark", default="SPY", help="Benchmark ticker symbol. Default: SPY.")
    parser.add_argument("--window-days", type=int, default=30, help="Cluster window in calendar days.")
    parser.add_argument("--cooldown-days", type=int, default=90, help="Cooldown in calendar days per ticker.")
    parser.add_argument("--min-distinct-insiders", type=int, default=2, help="Minimum insiders in the cluster.")
    parser.add_argument("--min-total-value", type=float, default=100000.0, help="Minimum cluster purchase value.")
    parser.add_argument("--min-price", type=float, default=5.0, help="Minimum entry price.")
    parser.add_argument(
        "--min-daily-dollar-volume",
        type=float,
        default=1000000.0,
        help="Minimum 20-day average daily dollar volume.",
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
) -> dict[str, object]:
    trades = load_insider_trades(insider_csv)
    if not trades:
        raise ValueError("No eligible insider trades were found after applying the open-market purchase filters.")

    bars_by_ticker = load_price_bars(prices_csv)
    if not bars_by_ticker:
        raise ValueError("No price bars were loaded from the prices CSV.")

    candidates = build_signal_candidates(
        trades=trades,
        window_days=window_days,
        min_distinct_insiders=min_distinct_insiders,
        min_total_value=min_total_value,
        cooldown_days=cooldown_days,
    )

    qualified_events, candidate_rows = qualify_events(
        candidates=candidates,
        bars_by_ticker=bars_by_ticker,
        benchmark_ticker=normalize_ticker(benchmark),
        min_price=min_price,
        min_daily_dollar_volume=min_daily_dollar_volume,
        lookback_days=lookback_days,
        min_market_cap=min_market_cap,
    )

    event_rows, summary_rows, segment_rows = build_output_tables(
        qualified_events=qualified_events,
        bars_by_ticker=bars_by_ticker,
        benchmark_ticker=normalize_ticker(benchmark),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "signal_candidates.csv", candidate_rows)
    write_csv(output_dir / "qualified_events.csv", event_rows)
    write_csv(output_dir / "results_summary.csv", summary_rows)
    write_csv(output_dir / "segmented_analysis.csv", segment_rows)
    build_markdown_summary(
        output_dir=output_dir,
        candidate_rows=candidate_rows,
        qualified_events=qualified_events,
        summary_rows=summary_rows,
        benchmark_ticker=normalize_ticker(benchmark),
    )

    return {
        "candidate_rows": candidate_rows,
        "qualified_events": qualified_events,
        "event_rows": event_rows,
        "summary_rows": summary_rows,
        "segment_rows": segment_rows,
        "candidate_count": len(candidate_rows),
        "qualified_count": len(qualified_events),
        "rejected_count": len(candidate_rows) - len(qualified_events),
        "output_dir": output_dir,
        "benchmark": normalize_ticker(benchmark),
    }


def build_console_summary(result: dict[str, object]) -> str:
    lines = [
        "Insider Event Study Complete",
        f"Output directory: {result['output_dir']}",
        f"Benchmark: {result['benchmark']}",
        f"Signal candidates: {result['candidate_count']}",
        f"Qualified events: {result['qualified_count']}",
        f"Rejected candidates: {result['rejected_count']}",
        "",
        "By Horizon",
    ]

    summary_rows = result["summary_rows"]
    for row in summary_rows:
        horizon = row["horizon_days"]
        lines.append(
            "  "
            + f"{horizon}d"
            + f" | complete={row['complete_event_count'] or '0'}"
            + f" | hit_rate={row['hit_rate'] or 'n/a'}"
            + f" | mean_return={row['mean_return'] or 'n/a'}"
            + f" | mean_excess={row['mean_excess_return'] or 'n/a'}"
        )
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
    )

    print(build_console_summary(result))
    print(f"\nWrote study outputs to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
