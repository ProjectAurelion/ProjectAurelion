from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

import data_pipeline
import price_loader


def test_master_index_fetch_uses_cache(tmp_path: Path, monkeypatch) -> None:
    calls = {"count": 0}

    def fake_request_bytes(url: str, user_agent: str) -> bytes:
        calls["count"] += 1
        return (
            "Description|Master Index|2024|QTR1|\n"
            "--------------------------------------\n"
            "0001234567|Example Corp|4|2024-01-05|edgar/data/1234567/test.txt\n"
        ).encode("latin-1")

    monkeypatch.setattr(data_pipeline, "request_bytes", fake_request_bytes)

    first = data_pipeline.fetch_master_index(2024, 1, "Test User", cache_root=tmp_path)
    second = data_pipeline.fetch_master_index(2024, 1, "Test User", cache_root=tmp_path)

    assert calls["count"] == 1
    assert len(first) == 1
    assert len(second) == 1


def test_price_history_fetch_uses_cache(tmp_path: Path, monkeypatch) -> None:
    calls = {"count": 0}

    def fake_request_json(url: str) -> dict:
        calls["count"] += 1
        return {
            "chart": {
                "result": [
                    {
                        "timestamp": [1704067200],
                        "indicators": {
                            "quote": [{"open": [10.0], "high": [10.0], "low": [10.0], "close": [10.0], "volume": [100]}],
                            "adjclose": [{"adjclose": [10.0]}],
                        },
                    }
                ]
            }
        }

    monkeypatch.setattr(price_loader, "_request_json", fake_request_json)

    first = price_loader.fetch_yahoo_history("TEST", data_pipeline.parse_date("2024-01-01"), data_pipeline.parse_date("2024-01-02"), tmp_path)
    second = price_loader.fetch_yahoo_history("TEST", data_pipeline.parse_date("2024-01-01"), data_pipeline.parse_date("2024-01-02"), tmp_path)

    assert calls["count"] == 1
    assert first[0]["adj_close"] == "10.0"
    assert second[0]["adj_close"] == "10.0"


def test_external_price_history_normalizes_vendor_csv_and_supplements_benchmark(tmp_path: Path, monkeypatch) -> None:
    input_csv = tmp_path / "vendor_prices.csv"
    output_csv = tmp_path / "normalized_prices.csv"

    with input_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "Symbol",
                "Date",
                "Open",
                "High",
                "Low",
                "Close",
                "Adjusted_Close",
                "Volume",
                "MarketCap",
                "SharesOutstanding",
                "Sector",
                "Industry",
                "Exchange",
                "Country",
                "Vendor",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Symbol": "TEST",
                "Date": "2024-01-03",
                "Open": "10",
                "High": "10",
                "Low": "10",
                "Close": "10",
                "Adjusted_Close": "10",
                "Volume": "1000",
                "MarketCap": "500000000",
                "SharesOutstanding": "50000000",
                "Sector": "Technology",
                "Industry": "Software",
                "Exchange": "NASDAQ",
                "Country": "US",
                "Vendor": "premium_vendor",
            }
        )

    def fake_fetch_yahoo_history(ticker, start_date, end_date, cache_root):
        assert ticker == "SPY"
        return [
            {
                "date": "2024-01-03",
                "open": "100",
                "high": "100",
                "low": "100",
                "close": "100",
                "adj_open": "100",
                "adj_high": "100",
                "adj_low": "100",
                "adj_close": "100",
                "volume": "1000000",
            }
        ]

    monkeypatch.setattr(data_pipeline, "fetch_yahoo_history", fake_fetch_yahoo_history)

    result = data_pipeline.normalize_external_price_history(
        input_csv=input_csv,
        benchmark_ticker="SPY",
        start_date=data_pipeline.parse_date("2024-01-01"),
        end_date=data_pipeline.parse_date("2024-01-31"),
        output_csv=output_csv,
        cache_root=tmp_path,
        vendor_profile="generic",
    )

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert result["input_mode"] == "external_csv"
    assert result["benchmark_supplemented"] == "yes"
    assert result["price_vendor_profile"] == "generic"
    assert {row["ticker"] for row in rows} == {"TEST", "SPY"}
    test_row = next(row for row in rows if row["ticker"] == "TEST")
    assert test_row["market_cap"] == "500000000"
    assert test_row["shares_outstanding"] == "50000000"
    assert test_row["sector"] == "Technology"
    assert test_row["industry"] == "Software"
    assert test_row["exchange"] == "NASDAQ"
    assert test_row["country"] == "US"
    assert test_row["price_source"] == "premium_vendor"


def test_external_price_history_supports_institutional_profile_and_derives_market_cap(
    tmp_path: Path,
    monkeypatch,
) -> None:
    input_csv = tmp_path / "institutional_prices.csv"
    output_csv = tmp_path / "normalized_prices.csv"

    with input_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "Instrument",
                "AsOfDate",
                "Px_Open",
                "Px_High",
                "Px_Low",
                "Px_Last",
                "Adj_Px_Close",
                "Px_Volume",
                "Shr_Out",
                "Gics_Sector_Name",
                "Gics_Sub_Industry_Name",
                "Primary_Exchange",
                "Country_Iso",
                "Vendor",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Instrument": "RICH",
                "AsOfDate": "20240103",
                "Px_Open": "10",
                "Px_High": "11",
                "Px_Low": "9",
                "Px_Last": "10",
                "Adj_Px_Close": "9.5",
                "Px_Volume": "2000",
                "Shr_Out": "1000000",
                "Gics_Sector_Name": "Industrials",
                "Gics_Sub_Industry_Name": "Machinery",
                "Primary_Exchange": "NYSE",
                "Country_Iso": "US",
                "Vendor": "inst_feed",
            }
        )

    def fake_fetch_yahoo_history(ticker, start_date, end_date, cache_root):
        assert ticker == "SPY"
        return []

    monkeypatch.setattr(data_pipeline, "fetch_yahoo_history", fake_fetch_yahoo_history)

    result = data_pipeline.normalize_external_price_history(
        input_csv=input_csv,
        benchmark_ticker="SPY",
        start_date=data_pipeline.parse_date("2024-01-01"),
        end_date=data_pipeline.parse_date("2024-01-31"),
        output_csv=output_csv,
        cache_root=tmp_path,
        vendor_profile="institutional",
    )

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert result["price_vendor_profile"] == "institutional"
    assert result["derived_market_cap_row_count"] == 1
    rich_row = next(row for row in rows if row["ticker"] == "RICH")
    assert rich_row["market_cap"] == "9500000"
    assert rich_row["adj_open"] == "9.5"
    assert rich_row["adj_high"] == "10.45"
    assert rich_row["adj_low"] == "8.55"
    assert rich_row["sector"] == "Industrials"
    assert rich_row["industry"] == "Machinery"
    assert rich_row["price_source"] == "inst_feed"


def test_external_price_history_supports_mapping_json_overrides(tmp_path: Path, monkeypatch) -> None:
    input_csv = tmp_path / "mapped_prices.csv"
    output_csv = tmp_path / "normalized_prices.csv"
    mapping_json = tmp_path / "vendor_mapping.json"

    with input_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ric", "as_of_date", "close_px", "volume_traded", "shares_basic"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ric": "MAPD",
                "as_of_date": "2024-01-03",
                "close_px": "25",
                "volume_traded": "1000",
                "shares_basic": "4000000",
            }
        )

    mapping_json.write_text(
        json.dumps(
            {
                "fields": {
                    "ticker": "ric",
                    "date": "as_of_date",
                    "close": "close_px",
                    "volume": "volume_traded",
                    "shares_outstanding": "shares_basic",
                },
                "constants": {
                    "price_source": "mapped_vendor",
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_fetch_yahoo_history(ticker, start_date, end_date, cache_root):
        assert ticker == "SPY"
        return []

    monkeypatch.setattr(data_pipeline, "fetch_yahoo_history", fake_fetch_yahoo_history)

    result = data_pipeline.normalize_external_price_history(
        input_csv=input_csv,
        benchmark_ticker="SPY",
        start_date=data_pipeline.parse_date("2024-01-01"),
        end_date=data_pipeline.parse_date("2024-01-31"),
        output_csv=output_csv,
        cache_root=tmp_path,
        vendor_profile="generic",
        mapping_json=mapping_json,
    )

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert result["price_mapping_json"] == str(mapping_json)
    assert result["derived_market_cap_row_count"] == 1
    mapped_row = next(row for row in rows if row["ticker"] == "MAPD")
    assert mapped_row["price_source"] == "mapped_vendor"
    assert mapped_row["market_cap"] == "100000000"
