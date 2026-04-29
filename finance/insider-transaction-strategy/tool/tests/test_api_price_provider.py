from __future__ import annotations

import sys
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

import api_price_provider


def test_fmp_history_bundle_uses_cache_and_enriches_rows(tmp_path: Path, monkeypatch) -> None:
    calls = {"count": 0}

    def fake_request_json(url: str):
        calls["count"] += 1
        if "historical-price-eod/full" in url:
            return [
                {
                    "date": "2024-01-03",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10,
                    "adjClose": 9.5,
                    "volume": 1000,
                }
            ]
        if "historical-market-capitalization" in url:
            return [
                {
                    "date": "2024-01-03",
                    "marketCap": 95_000_000,
                }
            ]
        if "profile" in url:
            return [
                {
                    "symbol": "APIX",
                    "sector": "Technology",
                    "industry": "Software",
                    "exchangeShortName": "NASDAQ",
                    "country": "US",
                    "mktCap": 100_000_000,
                    "price": 10,
                }
            ]
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(api_price_provider, "_request_json", fake_request_json)

    rows_first, meta_first = api_price_provider.fetch_fmp_history_bundle(
        "APIX",
        start_date=api_price_provider.date(2024, 1, 1),
        end_date=api_price_provider.date(2024, 1, 31),
        cache_root=tmp_path,
        api_key="demo",
    )
    rows_second, meta_second = api_price_provider.fetch_fmp_history_bundle(
        "APIX",
        start_date=api_price_provider.date(2024, 1, 1),
        end_date=api_price_provider.date(2024, 1, 31),
        cache_root=tmp_path,
        api_key="demo",
    )

    assert calls["count"] == 3
    assert rows_first == rows_second
    assert meta_first == meta_second
    assert rows_first[0]["market_cap"] == "95000000"
    assert rows_first[0]["shares_outstanding"] == "10000000"
    assert rows_first[0]["sector"] == "Technology"
    assert rows_first[0]["industry"] == "Software"
    assert rows_first[0]["exchange"] == "NASDAQ"
    assert rows_first[0]["country"] == "US"
    assert rows_first[0]["price_source"] == "fmp_api"
    assert meta_first["profile_available"] is True
    assert meta_first["historical_market_cap_available"] is True
    assert meta_first["derived_market_cap_row_count"] == 0


def test_fmp_history_bundle_derives_market_cap_from_profile_when_history_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_request_json(url: str):
        if "historical-price-eod/full" in url:
            return [
                {
                    "date": "2024-01-03",
                    "open": 12,
                    "high": 12,
                    "low": 12,
                    "close": 12,
                    "adjClose": 12,
                    "volume": 2000,
                }
            ]
        if "historical-market-capitalization" in url:
            return []
        if "profile" in url:
            return [
                {
                    "symbol": "FALL",
                    "sector": "Industrials",
                    "industry": "Machinery",
                    "exchangeShortName": "NYSE",
                    "country": "US",
                    "mktCap": 120_000_000,
                    "price": 12,
                }
            ]
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(api_price_provider, "_request_json", fake_request_json)

    rows, meta = api_price_provider.fetch_fmp_history_bundle(
        "FALL",
        start_date=api_price_provider.date(2024, 1, 1),
        end_date=api_price_provider.date(2024, 1, 31),
        cache_root=tmp_path,
        api_key="demo",
    )

    assert rows[0]["market_cap"] == "120000000"
    assert rows[0]["shares_outstanding"] == "10000000"
    assert meta["historical_market_cap_available"] is False
    assert meta["derived_market_cap_row_count"] == 1
