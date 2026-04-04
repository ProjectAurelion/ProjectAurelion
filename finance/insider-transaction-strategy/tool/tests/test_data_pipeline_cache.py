from __future__ import annotations

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
