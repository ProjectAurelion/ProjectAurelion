from __future__ import annotations

import os
import sys
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

import env_loader


def test_load_env_file_sets_missing_values_only(tmp_path: Path, monkeypatch) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "FMP_API_KEY=demo-key\nMARKET_DATA_API_KEY=secondary-key\n# comment\n\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.setenv("MARKET_DATA_API_KEY", "existing-key")

    loaded = env_loader.load_env_file(env_path)

    assert os.environ["FMP_API_KEY"] == "demo-key"
    assert os.environ["MARKET_DATA_API_KEY"] == "existing-key"
    assert loaded["FMP_API_KEY"] == "demo-key"
    assert loaded["MARKET_DATA_API_KEY"] == "existing-key"
