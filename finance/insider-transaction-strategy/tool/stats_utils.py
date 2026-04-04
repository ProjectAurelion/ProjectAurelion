#!/usr/bin/env python3
"""Statistical helpers for event-study summaries."""

from __future__ import annotations

import math
from typing import Optional

import numpy as np

try:
    from scipy import stats
except ModuleNotFoundError:  # pragma: no cover - exercised only when scipy is unavailable.
    stats = None


def percentile(values: list[float], pct: float) -> Optional[float]:
    if not values:
        return None
    return float(np.percentile(np.asarray(values, dtype=float), pct * 100.0))


def summarize_numeric(values: list[float]) -> dict[str, Optional[float]]:
    if not values:
        return {
            "mean": None,
            "median": None,
            "stddev": None,
            "p25": None,
            "p75": None,
        }
    array = np.asarray(values, dtype=float)
    return {
        "mean": float(np.mean(array)),
        "median": float(np.median(array)),
        "stddev": float(np.std(array, ddof=1)) if len(array) > 1 else 0.0,
        "p25": percentile(values, 0.25),
        "p75": percentile(values, 0.75),
    }


def sample_warning_flags(sample_size: int) -> list[str]:
    flags: list[str] = []
    if sample_size < 10:
        flags.append("insufficient_sample_for_inference")
    elif sample_size < 30:
        flags.append("small_sample_caution")
    return flags


def one_sample_ttest(values: list[float]) -> tuple[Optional[float], Optional[float]]:
    if len(values) < 10:
        return None, None
    if stats is None:
        array = np.asarray(values, dtype=float)
        stddev = np.std(array, ddof=1)
        if stddev == 0:
            return 0.0, 1.0
        t_stat = float(np.mean(array) / (stddev / math.sqrt(len(array))))
        return t_stat, None
    result = stats.ttest_1samp(np.asarray(values, dtype=float), 0.0, alternative="two-sided")
    return float(result.statistic), float(result.pvalue)


def bootstrap_mean_ci(
    values: list[float],
    *,
    n_boot: int = 2000,
    seed: int = 7,
) -> tuple[Optional[float], Optional[float]]:
    if len(values) < 10:
        return None, None
    rng = np.random.default_rng(seed)
    array = np.asarray(values, dtype=float)
    draws = rng.choice(array, size=(n_boot, len(array)), replace=True)
    means = draws.mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def format_number(value: Optional[float]) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return f"{value:.6f}"
