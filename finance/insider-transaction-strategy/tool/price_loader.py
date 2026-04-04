#!/usr/bin/env python3
"""Price loading helpers with adjusted close support and caching."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from cache_utils import cache_path, read_or_fetch_json

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"


def fetch_yahoo_history(
    ticker: str,
    start_date: date,
    end_date: date,
    cache_root: Path,
) -> list[dict[str, str]]:
    period1 = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    period2 = int(datetime.combine(end_date + timedelta(days=1), datetime.min.time()).timestamp())
    query = urllib.parse.urlencode(
        {
            "period1": period1,
            "period2": period2,
            "interval": "1d",
            "includeAdjustedClose": "true",
            "events": "div,splits",
        }
    )
    url = f"{YAHOO_CHART_URL}/{urllib.parse.quote(ticker)}?{query}"
    payload = read_or_fetch_json(
        cache_path(cache_root, "prices", url, ".json"),
        lambda: _request_json(url),
    )

    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        return []

    timestamps = result.get("timestamp") or []
    quotes = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adj = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    opens = quotes.get("open") or []
    highs = quotes.get("high") or []
    lows = quotes.get("low") or []
    closes = quotes.get("close") or []
    volumes = quotes.get("volume") or []

    rows: list[dict[str, str]] = []
    for index, timestamp in enumerate(timestamps):
        if index >= len(closes) or closes[index] is None:
            continue
        raw_close = closes[index]
        adj_close = adj[index] if index < len(adj) and adj[index] is not None else raw_close
        ratio = (adj_close / raw_close) if raw_close not in (None, 0) else 1.0
        trading_date = datetime.fromtimestamp(timestamp, UTC).date()
        rows.append(
            {
                "date": trading_date.isoformat(),
                "open": _string_value(opens, index),
                "high": _string_value(highs, index),
                "low": _string_value(lows, index),
                "close": str(raw_close),
                "adj_close": str(adj_close),
                "adj_open": _adjusted_string(opens, index, ratio),
                "adj_high": _adjusted_string(highs, index, ratio),
                "adj_low": _adjusted_string(lows, index, ratio),
                "volume": _string_value(volumes, index),
            }
        )
    return rows


def _request_json(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8", errors="ignore"))


def _string_value(values: list, index: int) -> str:
    if index >= len(values) or values[index] is None:
        return ""
    return str(values[index])


def _adjusted_string(values: list, index: int, ratio: float) -> str:
    if index >= len(values) or values[index] is None:
        return ""
    return str(values[index] * ratio)
