#!/usr/bin/env python3
"""Vendor price adapter profiles and normalization helpers."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

CANONICAL_PRICE_FIELDS = (
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
    "shares_outstanding",
    "sector",
    "industry",
    "exchange",
    "country",
    "price_source",
)

REQUIRED_FIELDS = ("ticker", "date", "close", "volume")
DEFAULT_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y%m%d",
    "%Y%m%d %H:%M:%S",
)


def snake_case(value: str) -> str:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value.strip())
    spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", spaced)
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in spaced).strip("_")


def normalize_ticker(value: str) -> str:
    return value.strip().upper()


def parse_flexible_date(value: str, extra_formats: tuple[str, ...] = ()) -> date:
    cleaned = value.strip()
    for fmt in extra_formats + DEFAULT_DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value}")


def parse_float(raw: str) -> Optional[float]:
    cleaned = raw.strip().replace("$", "").replace(",", "")
    if not cleaned or cleaned.lower() in {"na", "nan", "none", "null"}:
        return None
    return float(cleaned)


def format_float(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _dedupe_aliases(values: list[str]) -> tuple[str, ...]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = snake_case(value)
        if normalized and normalized not in seen:
            deduped.append(normalized)
            seen.add(normalized)
    return tuple(deduped)


def _default_profile_aliases() -> dict[str, tuple[str, ...]]:
    return {
        "ticker": _dedupe_aliases(
            [
                "ticker",
                "symbol",
                "security",
                "instrument",
            ]
        ),
        "date": _dedupe_aliases(
            [
                "date",
                "trading_date",
                "trade_date",
                "pricing_date",
                "pricedate",
                "as_of_date",
            ]
        ),
        "open": _dedupe_aliases(["open", "open_price", "px_open"]),
        "high": _dedupe_aliases(["high", "high_price", "px_high"]),
        "low": _dedupe_aliases(["low", "low_price", "px_low"]),
        "close": _dedupe_aliases(["close", "close_price", "px_last", "px_close", "last"]),
        "adj_open": _dedupe_aliases(["adj_open", "adjusted_open", "adj_px_open"]),
        "adj_high": _dedupe_aliases(["adj_high", "adjusted_high", "adj_px_high"]),
        "adj_low": _dedupe_aliases(["adj_low", "adjusted_low", "adj_px_low"]),
        "adj_close": _dedupe_aliases(
            [
                "adj_close",
                "adjusted_close",
                "adjclose",
                "adjusted_close_price",
                "adj_px_close",
            ]
        ),
        "volume": _dedupe_aliases(["volume", "shares_traded", "daily_volume", "px_volume"]),
        "market_cap": _dedupe_aliases(["market_cap", "marketcap", "mkt_cap", "cur_mkt_cap"]),
        "shares_outstanding": _dedupe_aliases(
            [
                "shares_outstanding",
                "sharesoutstanding",
                "shares_out",
                "shr_out",
                "shares_basic",
            ]
        ),
        "sector": _dedupe_aliases(["sector", "gics_sector", "gics_sector_name"]),
        "industry": _dedupe_aliases(
            [
                "industry",
                "gics_industry",
                "gics_industry_name",
                "subindustry",
                "gics_sub_industry_name",
            ]
        ),
        "exchange": _dedupe_aliases(["exchange", "primary_exchange", "listing_exchange", "mic"]),
        "country": _dedupe_aliases(["country", "domicile_country", "country_iso"]),
        "price_source": _dedupe_aliases(["price_source", "vendor", "source"]),
    }


PROFILE_ALIASES: dict[str, dict[str, tuple[str, ...]]] = {
    "generic": _default_profile_aliases(),
    "normalized": {field: (field,) for field in CANONICAL_PRICE_FIELDS},
    "institutional": {
        "ticker": _dedupe_aliases(["ticker", "symbol", "instrument", "security", "security_id"]),
        "date": _dedupe_aliases(["trade_date", "pricing_date", "as_of_date", "date"]),
        "open": _dedupe_aliases(["px_open", "open", "open_price"]),
        "high": _dedupe_aliases(["px_high", "high", "high_price"]),
        "low": _dedupe_aliases(["px_low", "low", "low_price"]),
        "close": _dedupe_aliases(["px_last", "px_close", "close", "close_price"]),
        "adj_open": _dedupe_aliases(["adj_px_open", "adj_open", "adjusted_open"]),
        "adj_high": _dedupe_aliases(["adj_px_high", "adj_high", "adjusted_high"]),
        "adj_low": _dedupe_aliases(["adj_px_low", "adj_low", "adjusted_low"]),
        "adj_close": _dedupe_aliases(["adj_px_close", "adj_close", "adjusted_close"]),
        "volume": _dedupe_aliases(["px_volume", "volume", "shares_traded"]),
        "market_cap": _dedupe_aliases(["cur_mkt_cap", "mkt_cap", "market_cap", "marketcap"]),
        "shares_outstanding": _dedupe_aliases(["shr_out", "shares_outstanding", "sharesoutstanding"]),
        "sector": _dedupe_aliases(["gics_sector_name", "gics_sector", "sector"]),
        "industry": _dedupe_aliases(
            [
                "gics_sub_industry_name",
                "gics_industry_name",
                "gics_industry",
                "industry",
            ]
        ),
        "exchange": _dedupe_aliases(["primary_exchange", "listing_exchange", "exchange", "mic"]),
        "country": _dedupe_aliases(["country_iso", "domicile_country", "country"]),
        "price_source": _dedupe_aliases(["vendor", "source", "price_source"]),
    },
}

PROFILE_DESCRIPTIONS = {
    "generic": "Flexible adapter for general CSV exports with common finance column names.",
    "normalized": "Strict adapter for files already using the tool's internal canonical schema.",
    "institutional": "Profile tuned for richer market-data exports with px_/gics_/mkt_cap style fields.",
}


@dataclass(frozen=True)
class VendorProfile:
    name: str
    description: str
    field_aliases: dict[str, tuple[str, ...]]
    constants: dict[str, str]
    date_formats: tuple[str, ...] = ()

    def value_for(self, row: dict[str, str], field: str) -> str:
        for alias in self.field_aliases.get(field, (field,)):
            value = row.get(alias, "")
            if value.strip():
                return value.strip()
        return self.constants.get(field, "")


def available_vendor_profiles() -> dict[str, str]:
    return PROFILE_DESCRIPTIONS.copy()


def _load_mapping_payload(mapping_json: Optional[Path]) -> tuple[dict[str, object], str]:
    if mapping_json is None:
        return {}, ""
    path = mapping_json.expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Vendor mapping JSON not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Vendor mapping JSON must contain an object at the top level.")
    return payload, str(path)


def _coerce_mapping_aliases(raw_value: object) -> tuple[str, ...]:
    if isinstance(raw_value, str):
        return _dedupe_aliases([raw_value])
    if isinstance(raw_value, list) and all(isinstance(item, str) for item in raw_value):
        return _dedupe_aliases(list(raw_value))
    raise ValueError("Field mappings must be a string or a list of strings.")


def build_vendor_profile(profile_name: str = "generic", mapping_json: Optional[Path] = None) -> VendorProfile:
    mapping_payload, mapping_path = _load_mapping_payload(mapping_json)
    mapped_profile = str(mapping_payload.get("profile", "")).strip().lower()
    effective_profile = mapped_profile or profile_name.strip().lower() or "generic"

    if effective_profile not in PROFILE_ALIASES:
        available = ", ".join(sorted(PROFILE_ALIASES))
        raise ValueError(f"Unknown vendor profile '{effective_profile}'. Available profiles: {available}.")

    aliases = {
        field: tuple(values)
        for field, values in PROFILE_ALIASES[effective_profile].items()
    }
    constants: dict[str, str] = {"price_source": "external_csv"}
    date_formats: tuple[str, ...] = ()

    fields_override = mapping_payload.get("fields", {})
    if fields_override:
        if not isinstance(fields_override, dict):
            raise ValueError("Vendor mapping JSON 'fields' entry must be an object.")
        for canonical_field, raw_aliases in fields_override.items():
            field_name = snake_case(str(canonical_field))
            if field_name not in CANONICAL_PRICE_FIELDS:
                raise ValueError(f"Unknown canonical price field in mapping JSON: {canonical_field}")
            aliases[field_name] = _coerce_mapping_aliases(raw_aliases)

    constants_override = mapping_payload.get("constants", {})
    if constants_override:
        if not isinstance(constants_override, dict):
            raise ValueError("Vendor mapping JSON 'constants' entry must be an object.")
        for key, value in constants_override.items():
            field_name = snake_case(str(key))
            if field_name not in CANONICAL_PRICE_FIELDS:
                raise ValueError(f"Unknown canonical price field in mapping JSON constants: {key}")
            constants[field_name] = str(value).strip()

    raw_formats = mapping_payload.get("date_formats", [])
    if raw_formats:
        if not isinstance(raw_formats, list) or not all(isinstance(item, str) for item in raw_formats):
            raise ValueError("Vendor mapping JSON 'date_formats' entry must be a list of strings.")
        date_formats = tuple(raw_formats)

    description = PROFILE_DESCRIPTIONS[effective_profile]
    if mapping_path:
        description = f"{description} Custom mapping overrides applied."
    return VendorProfile(
        name=effective_profile,
        description=description,
        field_aliases=aliases,
        constants=constants,
        date_formats=date_formats,
    )


def normalize_price_rows(
    *,
    input_csv: Path,
    start_date: date,
    end_date: date,
    lookback_padding_days: int = 60,
    forward_padding_days: int = 400,
    vendor_profile: str = "generic",
    mapping_json: Optional[Path] = None,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    if not input_csv.exists():
        raise FileNotFoundError(f"External price CSV not found: {input_csv}")

    padded_start = start_date - timedelta(days=lookback_padding_days)
    padded_end = end_date + timedelta(days=forward_padding_days)
    profile = build_vendor_profile(vendor_profile, mapping_json)

    rows: list[dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    downloaded: list[str] = []
    skipped_row_count = 0
    duplicate_row_count = 0
    derived_market_cap_row_count = 0
    derived_adjusted_ohlc_row_count = 0
    adjusted_price_row_count = 0
    market_cap_row_count = 0
    field_presence = {
        "adj_close": 0,
        "market_cap": 0,
        "shares_outstanding": 0,
        "sector": 0,
        "industry": 0,
    }

    with input_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("External prices CSV is missing a header row.")
        for raw_row in reader:
            row = {snake_case(key): (value or "").strip() for key, value in raw_row.items()}
            normalized_row = {field: profile.value_for(row, field) for field in CANONICAL_PRICE_FIELDS}

            normalized_row["ticker"] = normalize_ticker(normalized_row["ticker"])
            if not all(normalized_row[field] for field in REQUIRED_FIELDS):
                skipped_row_count += 1
                continue

            try:
                trading_date = parse_flexible_date(normalized_row["date"], profile.date_formats)
            except ValueError:
                skipped_row_count += 1
                continue
            if trading_date < padded_start or trading_date > padded_end:
                continue
            normalized_row["date"] = trading_date.isoformat()

            derived_adjusted = False
            close_value = parse_float(normalized_row["close"])
            adj_close_value = parse_float(normalized_row["adj_close"])
            if close_value and close_value != 0 and adj_close_value is not None:
                factor = adj_close_value / close_value
                for raw_field, adjusted_field in (("open", "adj_open"), ("high", "adj_high"), ("low", "adj_low")):
                    if normalized_row[raw_field] and not normalized_row[adjusted_field]:
                        base_value = parse_float(normalized_row[raw_field])
                        if base_value is not None:
                            normalized_row[adjusted_field] = format_float(base_value * factor)
                            derived_adjusted = True
            if derived_adjusted:
                derived_adjusted_ohlc_row_count += 1

            if not normalized_row["market_cap"]:
                shares_out = parse_float(normalized_row["shares_outstanding"])
                pricing_value = parse_float(normalized_row["adj_close"] or normalized_row["close"])
                if shares_out is not None and pricing_value is not None:
                    normalized_row["market_cap"] = format_float(shares_out * pricing_value)
                    derived_market_cap_row_count += 1

            if normalized_row["adj_close"]:
                adjusted_price_row_count += 1
                field_presence["adj_close"] += 1
            if normalized_row["market_cap"]:
                market_cap_row_count += 1
                field_presence["market_cap"] += 1
            if normalized_row["shares_outstanding"]:
                field_presence["shares_outstanding"] += 1
            if normalized_row["sector"]:
                field_presence["sector"] += 1
            if normalized_row["industry"]:
                field_presence["industry"] += 1

            if not normalized_row["price_source"]:
                normalized_row["price_source"] = profile.constants.get("price_source", "external_csv")

            key = (normalized_row["ticker"], normalized_row["date"])
            if key in seen_pairs:
                duplicate_row_count += 1
                continue
            seen_pairs.add(key)
            rows.append(normalized_row)
            if normalized_row["ticker"] and normalized_row["ticker"] not in downloaded:
                downloaded.append(normalized_row["ticker"])

    normalized_row_count = len(rows)
    coverage = {
        key: round((count / normalized_row_count) * 100, 2) if normalized_row_count else 0.0
        for key, count in field_presence.items()
    }

    rows.sort(key=lambda row: (row["ticker"], row["date"]))
    return rows, {
        "downloaded_tickers": downloaded,
        "adjusted_price_row_count": adjusted_price_row_count,
        "market_cap_row_count": market_cap_row_count,
        "input_mode": "external_csv",
        "input_csv": str(input_csv),
        "price_vendor_profile": profile.name,
        "price_vendor_description": profile.description,
        "price_mapping_json": str(mapping_json.expanduser()) if mapping_json else "",
        "skipped_row_count": skipped_row_count,
        "duplicate_row_count": duplicate_row_count,
        "derived_market_cap_row_count": derived_market_cap_row_count,
        "derived_adjusted_ohlc_row_count": derived_adjusted_ohlc_row_count,
        "field_coverage_pct": coverage,
    }
