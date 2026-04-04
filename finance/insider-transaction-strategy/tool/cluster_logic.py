#!/usr/bin/env python3
"""Sliding-window cluster generation and overlap-aware primary event selection."""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta


def build_raw_cluster_candidates(
    trades: list,
    *,
    window_days: int,
    min_distinct_insiders: int,
    min_total_value: float,
) -> list[dict[str, object]]:
    trades_by_issuer: dict[str, list] = defaultdict(list)
    for trade in trades:
        trades_by_issuer[trade.issuer_cik].append(trade)

    candidates: list[dict[str, object]] = []
    for issuer_cik, issuer_trades in trades_by_issuer.items():
        issuer_trades.sort(key=lambda trade: (trade.announcement_date, trade.owner_group_id, trade.accession))
        left = 0
        owner_counts: dict[str, int] = {}
        total_value = 0.0
        last_emitted_date = None

        for right, trade in enumerate(issuer_trades):
            total_value += trade.total_value
            owner_counts[trade.owner_group_id] = owner_counts.get(trade.owner_group_id, 0) + 1
            oldest_allowed = trade.announcement_date - timedelta(days=window_days)

            while issuer_trades[left].announcement_date < oldest_allowed:
                old_trade = issuer_trades[left]
                total_value -= old_trade.total_value
                owner_counts[old_trade.owner_group_id] -= 1
                if owner_counts[old_trade.owner_group_id] <= 0:
                    del owner_counts[old_trade.owner_group_id]
                left += 1

            distinct_count = len(owner_counts)
            if (
                distinct_count >= min_distinct_insiders
                and total_value >= min_total_value
                and trade.announcement_date != last_emitted_date
            ):
                window = issuer_trades[left : right + 1]
                candidates.append(
                    {
                        "issuer_cik": issuer_cik,
                        "ticker": trade.ticker,
                        "issuer_name": trade.issuer_name,
                        "event_date": trade.announcement_date,
                        "window_start": window[0].announcement_date,
                        "window_end": window[-1].announcement_date,
                        "distinct_insiders": distinct_count,
                        "total_purchase_value": total_value,
                        "canonical_roles": "; ".join(sorted({item.canonical_role for item in window if item.canonical_role})),
                        "owner_group_names": "; ".join(sorted({item.owner_group_name for item in window if item.owner_group_name})),
                        "timing_ambiguous": "yes" if any(item.timing_ambiguous for item in window) else "no",
                        "data_quality_flags": "; ".join(sorted({flag for item in window for flag in item.data_quality_flags if flag})),
                    }
                )
                last_emitted_date = trade.announcement_date
    return candidates


def assign_overlap_groups(
    candidates: list[dict[str, object]],
    *,
    cooldown_days: int,
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for candidate in candidates:
        grouped[str(candidate["issuer_cik"])].append(candidate)

    annotated: list[dict[str, object]] = []
    for issuer_cik, issuer_candidates in grouped.items():
        issuer_candidates.sort(key=lambda item: item["event_date"])
        group_index = 0
        start = 0
        while start < len(issuer_candidates):
            group = [issuer_candidates[start]]
            end = start + 1
            while end < len(issuer_candidates):
                previous = group[-1]["event_date"]
                current = issuer_candidates[end]["event_date"]
                if current <= previous + timedelta(days=cooldown_days):
                    group.append(issuer_candidates[end])
                    end += 1
                else:
                    break

            strongest = max(
                group,
                key=lambda item: (float(item["total_purchase_value"]), int(item["distinct_insiders"]), item["event_date"]),
            )
            group_id = f"{issuer_cik}-overlap-{group_index}"
            for item in group:
                row = dict(item)
                row["overlap_group_id"] = group_id
                row["overlap_group_size"] = len(group)
                row["is_primary_event"] = "yes" if item is strongest else "no"
                annotated.append(row)
            group_index += 1
            start = end
    annotated.sort(key=lambda item: (item["issuer_cik"], item["event_date"]))
    return annotated
