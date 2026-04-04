from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

from cluster_logic import assign_overlap_groups


def test_overlap_groups_do_not_chain_transitively() -> None:
    candidates = [
        {"issuer_cik": "1", "event_date": date(2024, 1, 1), "total_purchase_value": 100.0, "distinct_insiders": 2},
        {"issuer_cik": "1", "event_date": date(2024, 3, 30), "total_purchase_value": 200.0, "distinct_insiders": 2},
        {"issuer_cik": "1", "event_date": date(2024, 6, 27), "total_purchase_value": 300.0, "distinct_insiders": 2},
    ]

    rows = assign_overlap_groups(candidates, cooldown_days=90)

    assert rows[0]["overlap_group_id"] == rows[1]["overlap_group_id"]
    assert rows[2]["overlap_group_id"] != rows[1]["overlap_group_id"]
    assert rows[0]["is_primary_event"] == "yes"
    assert rows[1]["is_primary_event"] == "no"
    assert rows[1]["is_strongest_in_overlap_group"] == "yes"
