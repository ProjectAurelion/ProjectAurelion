#!/usr/bin/env python3
"""Small filesystem cache helpers for SEC and market data."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def stable_cache_name(key: str, suffix: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"{digest}{suffix}"


def cache_path(cache_root: Path, namespace: str, key: str, suffix: str) -> Path:
    return cache_root / namespace / stable_cache_name(key, suffix)


def read_or_fetch_bytes(path: Path, fetcher: Callable[[], bytes]) -> bytes:
    if path.exists():
        return path.read_bytes()
    payload = fetcher()
    ensure_parent(path)
    path.write_bytes(payload)
    return payload


def read_or_fetch_text(
    path: Path,
    fetcher: Callable[[], str],
    *,
    encoding: str = "utf-8",
) -> str:
    if path.exists():
        return path.read_text(encoding=encoding)
    payload = fetcher()
    ensure_parent(path)
    path.write_text(payload, encoding=encoding)
    return payload


def read_or_fetch_json(path: Path, fetcher: Callable[[], Any]) -> Any:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    payload = fetcher()
    ensure_parent(path)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload
