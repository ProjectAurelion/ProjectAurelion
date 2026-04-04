from __future__ import annotations

import csv
import math
import sys
from datetime import date, timedelta
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

import insider_event_study


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def test_run_study_uses_primary_events_and_bhar(tmp_path: Path) -> None:
    insider_csv = tmp_path / "insider.csv"
    prices_csv = tmp_path / "prices.csv"
    output_dir = tmp_path / "output"

    write_csv(
        insider_csv,
        [
            "accession",
            "filing_date",
            "acceptance_datetime",
            "ticker",
            "issuer_name",
            "issuer_cik",
            "owner_group_id",
            "owner_group_name",
            "canonical_role",
            "transaction_code",
            "transaction_classification",
            "eligible_for_signal",
            "security_type",
            "is_direct_ownership",
            "total_value",
            "data_quality_flags",
        ],
        [
            {
                "accession": "a1",
                "filing_date": "2024-01-02",
                "acceptance_datetime": "2024-01-02 18:00:00",
                "ticker": "TEST",
                "issuer_name": "Test Corp",
                "issuer_cik": "0000001",
                "owner_group_id": "owner-a",
                "owner_group_name": "Owner A",
                "canonical_role": "CEO",
                "transaction_code": "P",
                "transaction_classification": "open_market_purchase",
                "eligible_for_signal": "yes",
                "security_type": "Common Stock",
                "is_direct_ownership": "yes",
                "total_value": "60000",
                "data_quality_flags": "",
            },
            {
                "accession": "a2",
                "filing_date": "2024-01-10",
                "acceptance_datetime": "2024-01-10 18:00:00",
                "ticker": "TEST",
                "issuer_name": "Test Corp",
                "issuer_cik": "0000001",
                "owner_group_id": "owner-b",
                "owner_group_name": "Owner B",
                "canonical_role": "Director",
                "transaction_code": "P",
                "transaction_classification": "open_market_purchase",
                "eligible_for_signal": "yes",
                "security_type": "Common Stock",
                "is_direct_ownership": "yes",
                "total_value": "60000",
                "data_quality_flags": "",
            },
            {
                "accession": "a3",
                "filing_date": "2024-01-20",
                "acceptance_datetime": "2024-01-20 18:00:00",
                "ticker": "TEST",
                "issuer_name": "Test Corp",
                "issuer_cik": "0000001",
                "owner_group_id": "owner-c",
                "owner_group_name": "Owner C",
                "canonical_role": "CFO",
                "transaction_code": "P",
                "transaction_classification": "open_market_purchase",
                "eligible_for_signal": "yes",
                "security_type": "Common Stock",
                "is_direct_ownership": "yes",
                "total_value": "80000",
                "data_quality_flags": "",
            },
        ],
    )

    days = business_days(date(2023, 11, 20), 110)
    price_rows: list[dict[str, str]] = []
    for idx, trading_day in enumerate(days):
        test_close = 10.0 + (2.0 / 43.0) * idx
        spy_close = 100.0 + (5.0 / 43.0) * idx
        price_rows.append(
            {
                "ticker": "TEST",
                "date": trading_day.isoformat(),
                "open": f"{test_close:.6f}",
                "high": f"{test_close:.6f}",
                "low": f"{test_close:.6f}",
                "close": f"{test_close:.6f}",
                "adj_open": f"{test_close:.6f}",
                "adj_high": f"{test_close:.6f}",
                "adj_low": f"{test_close:.6f}",
                "adj_close": f"{test_close:.6f}",
                "volume": "200000",
                "market_cap": "",
            }
        )
        price_rows.append(
            {
                "ticker": "SPY",
                "date": trading_day.isoformat(),
                "open": f"{spy_close:.6f}",
                "high": f"{spy_close:.6f}",
                "low": f"{spy_close:.6f}",
                "close": f"{spy_close:.6f}",
                "adj_open": f"{spy_close:.6f}",
                "adj_high": f"{spy_close:.6f}",
                "adj_low": f"{spy_close:.6f}",
                "adj_close": f"{spy_close:.6f}",
                "volume": "1000000",
                "market_cap": "",
            }
        )

    write_csv(
        prices_csv,
        [
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
        ],
        price_rows,
    )

    result = insider_event_study.run_study(
        insider_csv=insider_csv,
        prices_csv=prices_csv,
        output_dir=output_dir,
        benchmark="SPY",
        window_days=30,
        cooldown_days=90,
        min_distinct_insiders=2,
        min_total_value=100000,
        min_price=5.0,
        min_daily_dollar_volume=1000000.0,
        lookback_days=20,
        min_market_cap=100000000.0,
    )

    assert result["candidate_count"] == 2
    assert result["qualified_raw_count"] == 2
    assert result["qualified_count"] == 1
    assert result["coverage"]["primary_investable_event_count"] == 1
    assert result["coverage"]["market_cap_filter_active"] is False
    assert "Market-cap filter is inactive" in " ".join(result["warnings"])
    assert "Formal inference is suppressed" in " ".join(result["warnings"])

    event_row = result["event_rows"][0]
    summary_21d = next(
        row for row in result["summary_rows"] if row["sample_name"] == "all_primary" and row["horizon_days"] == "21"
    )
    assert event_row["complete_21d"] == "yes"
    assert math.isclose(float(summary_21d["mean_bhar_return"]), float(event_row["bhar_return_21d"]), rel_tol=1e-9)
    assert float(event_row["net_bhar_return_21d"]) < float(event_row["bhar_return_21d"])
    assert event_row["investable_under_capacity"] == "yes"
    assert event_row["horizon_flag_21d"] == "complete"
    assert summary_21d["warning_flags"] == "insufficient_sample_for_inference"
