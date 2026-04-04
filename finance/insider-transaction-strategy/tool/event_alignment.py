#!/usr/bin/env python3
"""Date alignment and cumulative-return helpers for event studies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

import numpy as np


@dataclass(frozen=True)
class AlignedSeries:
    dates: list[date]
    ordinals: np.ndarray
    raw_open: np.ndarray
    raw_close: np.ndarray
    adj_open: np.ndarray
    adj_close: np.ndarray
    volume: np.ndarray
    market_cap: np.ndarray
    adv: np.ndarray
    adjusted_price_coverage: np.ndarray


def build_aligned_series(bars: list, lookback_days: int) -> AlignedSeries:
    dates = [bar.trading_date for bar in bars]
    ordinals = np.asarray([item.toordinal() for item in dates], dtype=np.int64)
    raw_open = np.asarray([np.nan if bar.raw_open is None else bar.raw_open for bar in bars], dtype=float)
    raw_close = np.asarray([bar.raw_close for bar in bars], dtype=float)
    adj_open = np.asarray([np.nan if bar.adj_open is None else bar.adj_open for bar in bars], dtype=float)
    adj_close = np.asarray([bar.adj_close for bar in bars], dtype=float)
    volume = np.asarray([bar.volume for bar in bars], dtype=float)
    market_cap = np.asarray([np.nan if bar.market_cap is None else float(bar.market_cap) for bar in bars], dtype=float)
    adjusted_price_coverage = np.asarray([1.0 if bar.has_adjusted_price else 0.0 for bar in bars], dtype=float)

    dollar_volume = raw_close * volume
    adv = np.full(len(bars), np.nan, dtype=float)
    if len(bars) > lookback_days:
        cumulative = np.concatenate(([0.0], np.cumsum(dollar_volume)))
        for index in range(lookback_days, len(bars)):
            total = cumulative[index] - cumulative[index - lookback_days]
            adv[index] = total / lookback_days

    return AlignedSeries(
        dates=dates,
        ordinals=ordinals,
        raw_open=raw_open,
        raw_close=raw_close,
        adj_open=adj_open,
        adj_close=adj_close,
        volume=volume,
        market_cap=market_cap,
        adv=adv,
        adjusted_price_coverage=adjusted_price_coverage,
    )


def next_trading_index_after(series: AlignedSeries, target_date: date) -> Optional[int]:
    index = int(np.searchsorted(series.ordinals, target_date.toordinal(), side="right"))
    if index >= len(series.dates):
        return None
    return index


def first_trading_index_on_or_after(series: AlignedSeries, target_date: date) -> Optional[int]:
    index = int(np.searchsorted(series.ordinals, target_date.toordinal(), side="left"))
    if index >= len(series.dates):
        return None
    return index


def horizon_total_return(
    series: AlignedSeries,
    entry_index: int,
    horizon: int,
    *,
    entry_price: float,
) -> tuple[float, int, bool]:
    exit_index = entry_index + horizon
    complete = exit_index < len(series.dates)
    realized_exit_index = exit_index if complete else len(series.dates) - 1
    exit_price = float(series.adj_close[realized_exit_index])
    if entry_price == 0:
        return 0.0, realized_exit_index, complete
    return (exit_price / entry_price) - 1.0, realized_exit_index, complete
