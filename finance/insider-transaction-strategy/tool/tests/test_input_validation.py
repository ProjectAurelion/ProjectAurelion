from __future__ import annotations

import csv
import sys
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

import input_validation


def test_validate_insider_csv_counts_usable_rows(tmp_path: Path) -> None:
    insider_csv = tmp_path / "insider.csv"
    with insider_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["Ticker", "Insider_Name", "Filing_Date", "Total_Value", "Transaction_Code"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Ticker": "AAA",
                "Insider_Name": "Jane Doe",
                "Filing_Date": "2024-01-03",
                "Total_Value": "150000",
                "Transaction_Code": "P",
            }
        )
        writer.writerow(
            {
                "Ticker": "BBB",
                "Insider_Name": "",
                "Filing_Date": "2024-01-04",
                "Total_Value": "200000",
                "Transaction_Code": "P",
            }
        )

    summary = input_validation.validate_insider_csv(insider_csv)

    assert summary["row_count"] == 2
    assert summary["usable_row_count"] == 1
    assert summary["unique_ticker_count"] == 2
    assert summary["errors"] == []


def test_build_preflight_summary_surfaces_overlap_and_benchmark_checks(tmp_path: Path) -> None:
    insider_csv = tmp_path / "insider.csv"
    prices_csv = tmp_path / "prices.csv"

    with insider_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "insider_name", "filing_date", "total_value", "transaction_code"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "AAA",
                "insider_name": "Jane Doe",
                "filing_date": "2024-01-03",
                "total_value": "150000",
                "transaction_code": "P",
            }
        )

    with prices_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "date", "close", "adj_close", "volume", "market_cap"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "AAA",
                "date": "2024-01-03",
                "close": "10",
                "adj_close": "10",
                "volume": "1000",
                "market_cap": "100000000",
            }
        )
        writer.writerow(
            {
                "ticker": "SPY",
                "date": "2024-01-03",
                "close": "100",
                "adj_close": "100",
                "volume": "1000000",
                "market_cap": "500000000000",
            }
        )

    summary = input_validation.build_preflight_summary(
        insider_csv=insider_csv,
        prices_csv=prices_csv,
        benchmark_ticker="SPY",
        insider_input_mode="uploaded_csv",
        price_input_mode="uploaded_csv",
    )

    assert summary["ready"] is True
    assert summary["crosscheck"]["ticker_overlap_count"] == 1
    assert summary["prices"]["benchmark_row_count"] == 1
    assert summary["errors"] == []
