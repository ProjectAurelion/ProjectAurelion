#!/usr/bin/env python3
"""Build a case-study verdict from event-study results."""

from __future__ import annotations

from typing import Optional


def parse_float(raw: str) -> Optional[float]:
    raw = str(raw or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_int(raw: str) -> int:
    raw = str(raw or "").strip()
    if not raw:
        return 0
    try:
        return int(float(raw))
    except ValueError:
        return 0


def format_pct(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value > 0 else ""
    return f"{sign}{value * 100:.{digits}f}%"


def sample_label(sample_name: str) -> str:
    return "Investable primary events" if sample_name == "investable_primary" else "All primary events"


def _summary_rows_for_sample(summary_rows: list[dict[str, str]], sample_name: str) -> list[dict[str, str]]:
    rows = [row for row in summary_rows if row.get("sample_name") == sample_name]
    rows.sort(key=lambda row: parse_int(row.get("horizon_days", "")))
    return rows


def _pick_focus_sample(summary_rows: list[dict[str, str]]) -> str:
    investable = _summary_rows_for_sample(summary_rows, "investable_primary")
    if any(parse_int(row.get("complete_event_count", "")) > 0 for row in investable):
        return "investable_primary"
    return "all_primary"


def _pick_reference_row(rows: list[dict[str, str]]) -> Optional[dict[str, str]]:
    preferred_order = (63, 126, 21, 252)
    for horizon in preferred_order:
        for row in rows:
            if parse_int(row.get("horizon_days", "")) == horizon and parse_int(row.get("complete_event_count", "")) > 0:
                return row
    for row in rows:
        if parse_int(row.get("complete_event_count", "")) > 0:
            return row
    return None


def _row_for_horizon(rows: list[dict[str, str]], horizon: int) -> Optional[dict[str, str]]:
    for row in rows:
        if parse_int(row.get("horizon_days", "")) == horizon:
            return row
    return None


def build_case_study(
    *,
    benchmark: str,
    summary_rows: list[dict[str, str]],
    coverage: dict[str, object],
    warnings: list[str],
    parameter_outcome_rows: list[dict[str, str]],
    ticker_summary_rows: list[dict[str, str]],
    methodology: dict[str, object],
) -> dict[str, object]:
    focus_sample = _pick_focus_sample(summary_rows)
    focus_rows = _summary_rows_for_sample(summary_rows, focus_sample)
    reference_row = _pick_reference_row(focus_rows)
    row_63 = _row_for_horizon(focus_rows, 63)
    row_126 = _row_for_horizon(focus_rows, 126)
    row_252 = _row_for_horizon(focus_rows, 252)

    reference_complete = parse_int(reference_row.get("complete_event_count", "")) if reference_row else 0
    reference_horizon = parse_int(reference_row.get("horizon_days", "")) if reference_row else 0
    reference_mean = parse_float(reference_row.get("mean_net_bhar_return", "")) if reference_row else None
    reference_median = parse_float(reference_row.get("median_net_bhar_return", "")) if reference_row else None
    reference_positive_rate = parse_float(reference_row.get("positive_net_bhar_rate", "")) if reference_row else None
    mean_63 = parse_float(row_63.get("mean_net_bhar_return", "")) if row_63 else None
    mean_126 = parse_float(row_126.get("mean_net_bhar_return", "")) if row_126 else None
    mean_252 = parse_float(row_252.get("mean_net_bhar_return", "")) if row_252 else None
    investable_pct = float(coverage.get("primary_investable_pct", 0.0) or 0.0) / 100.0

    if reference_row is None or reference_complete == 0:
        verdict = "not_testable"
        headline = "The current run does not yet provide a usable answer."
        interpretation = "There were no complete forward-return horizons in the focus sample, so the tool cannot yet say whether delayed Form 4 tracking is viable under these rules."
    elif reference_complete < 10:
        verdict = "too_early"
        headline = "The signal is still too early to judge with confidence."
        interpretation = (
            f"Only {reference_complete} complete {reference_horizon}-day events were available in the focus sample, "
            "so this run is directional rather than reliable for judging whether delayed Form 4 tracking is viable."
        )
    else:
        medium_horizons = [value for value in (mean_63, mean_126, mean_252) if value is not None]
        positive_medium = sum(1 for value in medium_horizons if value > 0)
        if (reference_mean or 0.0) <= 0 and (mean_126 or reference_mean or 0.0) <= 0:
            verdict = "weak"
            headline = "The current rule set does not look like a strong delayed Form 4 trading signal."
            interpretation = (
                f"The focus sample lagged {benchmark} on average at the reference horizon "
                f"({format_pct(reference_mean)}), and the medium-term horizons do not show consistent outperformance."
            )
        elif (
            (reference_mean or 0.0) > 0
            and (reference_positive_rate or 0.0) >= 0.5
            and positive_medium >= max(1, len(medium_horizons) - 1)
        ):
            if reference_complete < 30:
                verdict = "promising_but_early"
                headline = "The signal looks promising, but the sample is still early."
                interpretation = (
                    f"The focus sample beat {benchmark} by {format_pct(reference_mean)} on average at {reference_horizon}d, "
                    "but the complete-event count is still below the point where you should trust it as robust."
                )
            else:
                verdict = "promising"
                headline = "The signal looks directionally viable under the current delayed-entry assumptions."
                interpretation = (
                    f"The focus sample beat {benchmark} by {format_pct(reference_mean)} on average at {reference_horizon}d, "
                    "and the medium-term horizons are broadly supportive."
                )
        else:
            verdict = "mixed"
            headline = "The signal is mixed under the current parameter set."
            interpretation = (
                f"Some evidence is positive, but the delayed-entry edge is not consistent enough yet to call it a clearly viable trading approach."
            )

    score = 0
    if reference_row is not None:
        score += min(reference_complete, 40)
        score += 20 if (reference_mean or 0.0) > 0 else 0
        score += 15 if (reference_positive_rate or 0.0) >= 0.5 else 0
        score += 10 if investable_pct >= 0.5 else 0
        score += 15 if (mean_126 or 0.0) > 0 else 0
    score = max(0, min(100, score))

    top_winners = sorted(
        (row for row in parameter_outcome_rows if parse_float(row.get("reference_net_bhar_return", "")) is not None),
        key=lambda row: parse_float(row["reference_net_bhar_return"]) or 0.0,
        reverse=True,
    )[:3]
    top_losers = sorted(
        (row for row in parameter_outcome_rows if parse_float(row.get("reference_net_bhar_return", "")) is not None),
        key=lambda row: parse_float(row["reference_net_bhar_return"]) or 0.0,
    )[:3]

    evidence = [
        f"Focus sample: {sample_label(focus_sample)}.",
        f"Reference horizon: {reference_horizon} trading days." if reference_horizon else "Reference horizon: none yet.",
        f"Complete events at the reference horizon: {reference_complete}.",
        f"Average net edge vs {benchmark}: {format_pct(reference_mean)}.",
        f"Median net edge vs {benchmark}: {format_pct(reference_median)}.",
        f"Positive-rate at the reference horizon: {format_pct(reference_positive_rate)}.",
        f"Investable primary-event share: {investable_pct * 100:.1f}%.",
    ]
    next_steps = []
    if reference_complete < 10:
        next_steps.append("Expand the date range or combine more periods before trusting the conclusion.")
    if float(coverage.get("candidate_market_cap_coverage_pct", 0.0) or 0.0) == 0.0:
        next_steps.append("Add market-cap coverage so size filters and microcap separation become trustworthy.")
    if float(coverage.get("primary_investable_pct", 0.0) or 0.0) < 70.0:
        next_steps.append("Check whether the apparent edge survives after filtering to names that are truly investable at your assumed size.")
    if not next_steps:
        next_steps.append("Run the same study across additional periods to see whether the result is stable out of sample.")

    return {
        "verdict": verdict,
        "headline": headline,
        "interpretation": interpretation,
        "confidence_score_0_to_100": score,
        "benchmark": benchmark,
        "focus_sample": focus_sample,
        "focus_sample_label": sample_label(focus_sample),
        "reference_horizon_days": reference_horizon,
        "reference_complete_event_count": reference_complete,
        "reference_mean_net_bhar_return": reference_mean,
        "reference_median_net_bhar_return": reference_median,
        "reference_positive_rate": reference_positive_rate,
        "mean_net_bhar_63d": mean_63,
        "mean_net_bhar_126d": mean_126,
        "mean_net_bhar_252d": mean_252,
        "investable_primary_pct": investable_pct,
        "primary_qualified_event_count": int(coverage.get("primary_qualified_event_count", 0) or 0),
        "primary_investable_event_count": int(coverage.get("primary_investable_event_count", 0) or 0),
        "candidate_count": int(coverage.get("raw_candidate_count", 0) or 0),
        "methodology": methodology,
        "warnings": warnings,
        "evidence": evidence,
        "next_steps": next_steps,
        "top_reference_winners": top_winners,
        "top_reference_losers": top_losers,
        "ticker_summary_preview": ticker_summary_rows[:10],
    }


def build_case_study_markdown(case_study: dict[str, object]) -> str:
    lines = [
        "# Form 4 Trading Case Study",
        "",
        "## Core Question",
        "",
        "Does tracking clustered Form 4 insider purchases appear to be a viable trading signal after delayed public disclosure and simple trading frictions?",
        "",
        "## Verdict",
        "",
        f"* Verdict: `{case_study['verdict']}`",
        f"* Headline: {case_study['headline']}",
        f"* Benchmark: `{case_study['benchmark']}`",
        f"* Focus sample: `{case_study['focus_sample_label']}`",
        f"* Reference horizon: `{case_study['reference_horizon_days']}d`" if case_study.get("reference_horizon_days") else "* Reference horizon: `n/a`",
        f"* Confidence score: `{case_study['confidence_score_0_to_100']}/100`",
        "",
        "## Interpretation",
        "",
        str(case_study["interpretation"]),
        "",
        "## Key Evidence",
        "",
    ]
    lines.extend(f"* {item}" for item in case_study.get("evidence", []))
    lines.extend(
        [
            "",
            "## Warnings",
            "",
        ]
    )
    warnings = case_study.get("warnings", [])
    if warnings:
        lines.extend(f"* {item}" for item in warnings)
    else:
        lines.append("* No major warnings were raised.")
    lines.extend(
        [
            "",
            "## Next Steps",
            "",
        ]
    )
    lines.extend(f"* {item}" for item in case_study.get("next_steps", []))
    return "\n".join(lines) + "\n"
