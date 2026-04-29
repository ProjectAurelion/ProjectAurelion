#!/usr/bin/env python3
"""API-backed market-data helpers for richer historical price research."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Optional

from cache_utils import cache_path, read_or_fetch_json

FMP_STABLE_BASE_URL = "https://financialmodelingprep.com/stable"


def normalize_ticker(value: str) -> str:
    return value.strip().upper()


def parse_float(raw: object) -> Optional[float]:
    if raw is None:
        return None
    cleaned = str(raw).strip().replace("$", "").replace(",", "")
    if not cleaned or cleaned.lower() in {"na", "nan", "none", "null"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def format_float(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def available_market_data_providers() -> dict[str, str]:
    return {
        "yahoo_public": "Public Yahoo Finance chart API with adjusted-price support but limited reference data.",
        "fmp_api": "Financial Modeling Prep API with historical prices plus richer market-cap and company-profile enrichment.",
    }


def fetch_fmp_history_bundle(
    ticker: str,
    start_date: date,
    end_date: date,
    cache_root: Path,
    *,
    api_key: str,
    base_url: str = FMP_STABLE_BASE_URL,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    if not api_key.strip():
        raise ValueError("FMP API mode requires an API key.")

    normalized_ticker = normalize_ticker(ticker)
    history_rows = fetch_fmp_eod_history(
        normalized_ticker,
        start_date=start_date,
        end_date=end_date,
        cache_root=cache_root,
        api_key=api_key,
        base_url=base_url,
    )
    if not history_rows:
        return [], {
            "profile_available": False,
            "historical_market_cap_available": False,
            "derived_market_cap_row_count": 0,
        }

    profile: dict[str, object] = {}
    historical_market_caps: dict[str, float] = {}
    try:
        profile = fetch_fmp_profile(
            normalized_ticker,
            cache_root=cache_root,
            api_key=api_key,
            base_url=base_url,
        )
    except Exception:
        profile = {}

    try:
        historical_market_caps = fetch_fmp_historical_market_cap(
            normalized_ticker,
            start_date=start_date,
            end_date=end_date,
            cache_root=cache_root,
            api_key=api_key,
            base_url=base_url,
        )
    except Exception:
        historical_market_caps = {}

    sector = _profile_value(profile, "sector")
    industry = _profile_value(profile, "industry")
    exchange = _profile_value(profile, "exchangeShortName", "exchange", "exchangeShortName")
    country = _profile_value(profile, "country")
    profile_market_cap = parse_float(_profile_value(profile, "mktCap", "marketCap"))
    profile_price = parse_float(_profile_value(profile, "price"))
    profile_shares_outstanding = parse_float(_profile_value(profile, "sharesOutstanding"))
    if profile_shares_outstanding is None and profile_market_cap and profile_price and profile_price > 0:
        profile_shares_outstanding = profile_market_cap / profile_price

    derived_market_cap_row_count = 0
    normalized_rows: list[dict[str, str]] = []
    for row in history_rows:
        adj_close_value = parse_float(row.get("adj_close")) or parse_float(row.get("close"))
        market_cap_value = historical_market_caps.get(row["date"])
        shares_outstanding_value: Optional[float] = None

        if market_cap_value is not None and adj_close_value and adj_close_value > 0:
            shares_outstanding_value = market_cap_value / adj_close_value
        elif profile_shares_outstanding and adj_close_value and adj_close_value > 0:
            shares_outstanding_value = profile_shares_outstanding
            market_cap_value = shares_outstanding_value * adj_close_value
            derived_market_cap_row_count += 1

        normalized_rows.append(
            {
                "ticker": normalized_ticker,
                "date": row["date"],
                "open": row.get("open", ""),
                "high": row.get("high", ""),
                "low": row.get("low", ""),
                "close": row.get("close", ""),
                "adj_open": row.get("adj_open", ""),
                "adj_high": row.get("adj_high", ""),
                "adj_low": row.get("adj_low", ""),
                "adj_close": row.get("adj_close", ""),
                "volume": row.get("volume", ""),
                "market_cap": format_float(market_cap_value) if market_cap_value is not None else "",
                "shares_outstanding": (
                    format_float(shares_outstanding_value) if shares_outstanding_value is not None else ""
                ),
                "sector": sector,
                "industry": industry,
                "exchange": exchange,
                "country": country,
                "price_source": "fmp_api",
            }
        )

    return normalized_rows, {
        "profile_available": bool(profile),
        "historical_market_cap_available": bool(historical_market_caps),
        "derived_market_cap_row_count": derived_market_cap_row_count,
    }


def fetch_fmp_eod_history(
    ticker: str,
    *,
    start_date: date,
    end_date: date,
    cache_root: Path,
    api_key: str,
    base_url: str = FMP_STABLE_BASE_URL,
) -> list[dict[str, str]]:
    url = _build_url(
        base_url,
        "historical-price-eod/full",
        {
            "symbol": ticker,
            "from": start_date.isoformat(),
            "to": end_date.isoformat(),
            "apikey": api_key,
        },
    )
    payload = _read_json(url, cache_root, "prices/fmp/history")
    entries = _extract_sequence(payload, "historical", "data", "results")

    rows: list[dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        trading_date = str(entry.get("date", "")).strip()
        if not trading_date:
            continue
        close_value = parse_float(entry.get("close"))
        if close_value is None:
            continue
        adj_close_value = (
            parse_float(entry.get("adjClose"))
            or parse_float(entry.get("adj_close"))
            or parse_float(entry.get("adjustedClose"))
            or close_value
        )
        ratio = (adj_close_value / close_value) if close_value not in (None, 0) else 1.0
        open_value = parse_float(entry.get("open"))
        high_value = parse_float(entry.get("high"))
        low_value = parse_float(entry.get("low"))

        rows.append(
            {
                "date": trading_date,
                "open": format_float(open_value) if open_value is not None else "",
                "high": format_float(high_value) if high_value is not None else "",
                "low": format_float(low_value) if low_value is not None else "",
                "close": format_float(close_value),
                "adj_close": format_float(adj_close_value),
                "adj_open": format_float(open_value * ratio) if open_value is not None else "",
                "adj_high": format_float(high_value * ratio) if high_value is not None else "",
                "adj_low": format_float(low_value * ratio) if low_value is not None else "",
                "volume": _string_value(entry.get("volume")),
            }
        )

    rows.sort(key=lambda row: row["date"])
    return rows


def fetch_fmp_profile(
    ticker: str,
    *,
    cache_root: Path,
    api_key: str,
    base_url: str = FMP_STABLE_BASE_URL,
) -> dict[str, object]:
    url = _build_url(
        base_url,
        "profile",
        {
            "symbol": ticker,
            "apikey": api_key,
        },
    )
    payload = _read_json(url, cache_root, "prices/fmp/profile")
    entries = _extract_sequence(payload, "data", "results")
    if entries and isinstance(entries[0], dict):
        return entries[0]
    if isinstance(payload, dict):
        return payload
    return {}


def fetch_fmp_historical_market_cap(
    ticker: str,
    *,
    start_date: date,
    end_date: date,
    cache_root: Path,
    api_key: str,
    base_url: str = FMP_STABLE_BASE_URL,
) -> dict[str, float]:
    url = _build_url(
        base_url,
        "historical-market-capitalization",
        {
            "symbol": ticker,
            "from": start_date.isoformat(),
            "to": end_date.isoformat(),
            "apikey": api_key,
        },
    )
    payload = _read_json(url, cache_root, "prices/fmp/market-cap")
    entries = _extract_sequence(payload, "historical", "data", "results")
    market_caps: dict[str, float] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        trading_date = str(entry.get("date", "")).strip()
        market_cap_value = parse_float(entry.get("marketCap") or entry.get("market_cap"))
        if trading_date and market_cap_value is not None:
            market_caps[trading_date] = market_cap_value
    return market_caps


def _build_url(base_url: str, path: str, params: dict[str, object]) -> str:
    query = urllib.parse.urlencode(
        {
            key: value
            for key, value in params.items()
            if value is not None and str(value).strip()
        }
    )
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}?{query}"


def _read_json(url: str, cache_root: Path, namespace: str) -> object:
    return read_or_fetch_json(
        cache_path(cache_root, namespace, url, ".json"),
        lambda: _request_json(url),
    )


def _request_json(url: str) -> object:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Codex Insider Event Study/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    if isinstance(payload, dict):
        error_message = str(payload.get("Error Message") or payload.get("error") or "").strip()
        if error_message:
            raise ValueError(error_message)
    return payload


def _extract_sequence(payload: object, *keys: str) -> list[object]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in keys:
        values = payload.get(key)
        if isinstance(values, list):
            return values
    return []


def _profile_value(profile: dict[str, object], *names: str) -> str:
    for name in names:
        value = profile.get(name)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _string_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()
