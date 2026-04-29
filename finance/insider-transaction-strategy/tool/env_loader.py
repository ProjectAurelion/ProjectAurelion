#!/usr/bin/env python3
"""Tiny .env loader so local runs can use API keys without extra dependencies."""

from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: Path) -> dict[str, str]:
    loaded: dict[str, str] = {}
    if not path.exists() or not path.is_file():
        return loaded

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_key = key.strip()
        env_value = value.strip().strip('"').strip("'")
        if not env_key:
            continue
        if env_key not in os.environ:
            os.environ[env_key] = env_value
        loaded[env_key] = os.environ[env_key]
    return loaded
