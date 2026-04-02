#!/usr/bin/env python3
"""Download Form 4 insider transactions and daily price data for the event study."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

SEC_ARCHIVES_URL = "https://www.sec.gov/Archives"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
FORM_TYPES = {"4", "4/A"}


@dataclass(frozen=True)
class FilingEntry:
    cik: str
    company_name: str
    form_type: str
    filing_date: date
    path: str


def parse_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def normalize_ticker(value: str) -> str:
    return value.strip().upper()


def parse_ticker_filter(raw: str) -> set[str]:
    if not raw.strip():
        return set()
    tokens = re.split(r"[\s,]+", raw.strip())
    return {normalize_ticker(token) for token in tokens if token.strip()}


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def first_child(element: Optional[ET.Element], name: str) -> Optional[ET.Element]:
    if element is None:
        return None
    for child in list(element):
        if local_name(child.tag) == name:
            return child
    return None


def nested_text(element: Optional[ET.Element], *names: str) -> str:
    current = element
    for name in names:
        current = first_child(current, name)
        if current is None:
            return ""
    return (current.text or "").strip()


def descendants(element: ET.Element, name: str) -> Iterable[ET.Element]:
    for child in element.iter():
        if local_name(child.tag) == name:
            yield child


def request_bytes(url: str, user_agent: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        data = response.read()
        content_encoding = response.headers.get("Content-Encoding", "")
        if "gzip" in content_encoding.lower():
            return gzip.decompress(data)
        return data


def iter_quarters(start_date: date, end_date: date) -> Iterable[tuple[int, int]]:
    year = start_date.year
    quarter = ((start_date.month - 1) // 3) + 1
    end_key = (end_date.year, ((end_date.month - 1) // 3) + 1)
    while (year, quarter) <= end_key:
        yield year, quarter
        if quarter == 4:
            year += 1
            quarter = 1
        else:
            quarter += 1


def fetch_master_index(year: int, quarter: int, user_agent: str) -> list[FilingEntry]:
    url = f"{SEC_ARCHIVES_URL}/edgar/full-index/{year}/QTR{quarter}/master.gz"
    raw = request_bytes(url, user_agent)
    text = gzip.decompress(raw).decode("latin-1") if raw[:2] == b"\x1f\x8b" else raw.decode("latin-1")
    lines = text.splitlines()
    start_index = 0
    for index, line in enumerate(lines):
        if line.startswith("-----"):
            start_index = index + 1
            break

    entries: list[FilingEntry] = []
    for line in lines[start_index:]:
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company_name, form_type, filed_at, path = parts
        if form_type not in FORM_TYPES:
            continue
        entries.append(
            FilingEntry(
                cik=cik,
                company_name=company_name,
                form_type=form_type,
                filing_date=parse_date(filed_at),
                path=path,
            )
        )
    return entries


def extract_form4_xml(filing_text: str) -> str:
    matches = re.findall(r"<XML>(.*?)</XML>", filing_text, flags=re.DOTALL | re.IGNORECASE)
    for match in matches:
        if "<ownershipdocument" in match.lower():
            return match.strip()
    if "<ownershipdocument" in filing_text.lower():
        start = filing_text.lower().find("<ownershipdocument")
        end = filing_text.lower().rfind("</ownershipdocument>")
        if start != -1 and end != -1:
            end += len("</ownershipdocument>")
            return filing_text[start:end].strip()
    raise ValueError("Could not locate an ownershipDocument XML segment in the filing.")


def build_owner_role(owner_relationship: Optional[ET.Element]) -> str:
    if owner_relationship is None:
        return ""

    parts: list[str] = []
    if nested_text(owner_relationship, "isDirector") == "1":
        parts.append("Director")
    if nested_text(owner_relationship, "isOfficer") == "1":
        officer_title = nested_text(owner_relationship, "officerTitle")
        parts.append(officer_title or "Officer")
    if nested_text(owner_relationship, "isTenPercentOwner") == "1":
        parts.append("10% Owner")
    if nested_text(owner_relationship, "isOther") == "1":
        parts.append(nested_text(owner_relationship, "otherText") or "Other")
    return "; ".join(dict.fromkeys(part for part in parts if part))


def parse_form4_rows(
    *,
    filing_entry: FilingEntry,
    source_url: str,
    xml_text: str,
    ticker_filter: set[str],
) -> list[dict[str, str]]:
    root = ET.fromstring(xml_text)
    ticker = normalize_ticker(nested_text(root, "issuer", "issuerTradingSymbol"))
    if not ticker:
        return []
    if ticker_filter and ticker not in ticker_filter:
        return []

    issuer_name = nested_text(root, "issuer", "issuerName")
    issuer_cik = nested_text(root, "issuer", "issuerCik")

    owners: list[dict[str, str]] = []
    for owner in descendants(root, "reportingOwner"):
        owner_name = nested_text(owner, "reportingOwnerId", "rptOwnerName")
        owner_cik = nested_text(owner, "reportingOwnerId", "rptOwnerCik")
        role = build_owner_role(first_child(owner, "reportingOwnerRelationship"))
        owners.append(
            {
                "insider_id": owner_cik or owner_name,
                "insider_name": owner_name,
                "insider_role": role,
            }
        )

    if not owners:
        return []

    rows: list[dict[str, str]] = []
    for transaction in descendants(root, "nonDerivativeTransaction"):
        transaction_code = nested_text(transaction, "transactionCoding", "transactionCode").upper()
        acquired_disposed = nested_text(
            transaction,
            "transactionAmounts",
            "transactionAcquiredDisposedCode",
            "value",
        ).upper()
        if transaction_code != "P":
            continue
        if acquired_disposed and acquired_disposed != "A":
            continue

        security_type = nested_text(transaction, "securityTitle", "value")
        transaction_date = nested_text(transaction, "transactionDate", "value")
        shares = nested_text(transaction, "transactionAmounts", "transactionShares", "value")
        price = nested_text(transaction, "transactionAmounts", "transactionPricePerShare", "value")

        shares_float = float(shares) if shares else None
        price_float = float(price) if price else None
        total_value = shares_float * price_float if shares_float is not None and price_float is not None else None

        for owner in owners:
            rows.append(
                {
                    "filing_date": filing_entry.filing_date.isoformat(),
                    "transaction_date": transaction_date,
                    "ticker": ticker,
                    "issuer_name": issuer_name,
                    "issuer_cik": issuer_cik,
                    "insider_id": owner["insider_id"],
                    "insider_name": owner["insider_name"],
                    "insider_role": owner["insider_role"],
                    "transaction_code": transaction_code,
                    "transaction_type": "Open Market Purchase",
                    "security_type": security_type,
                    "shares": shares,
                    "price": price,
                    "total_value": f"{total_value:.6f}" if total_value is not None else "",
                    "source_url": source_url,
                }
            )
    return rows


def fetch_filing_transactions(
    filing_entry: FilingEntry,
    user_agent: str,
    ticker_filter: set[str],
) -> list[dict[str, str]]:
    source_url = f"{SEC_ARCHIVES_URL}/{filing_entry.path}"
    filing_text = request_bytes(source_url, user_agent).decode("utf-8", errors="ignore")
    xml_text = extract_form4_xml(filing_text)
    return parse_form4_rows(
        filing_entry=filing_entry,
        source_url=source_url,
        xml_text=xml_text,
        ticker_filter=ticker_filter,
    )


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def download_form4_transactions(
    *,
    start_date: date,
    end_date: date,
    output_csv: Path,
    user_agent: str,
    ticker_filter: set[str],
    max_filings: Optional[int] = None,
    pause_seconds: float = 0.2,
) -> dict[str, object]:
    filing_entries: list[FilingEntry] = []
    for year, quarter in iter_quarters(start_date, end_date):
        filing_entries.extend(fetch_master_index(year, quarter, user_agent))

    filing_entries = [
        entry for entry in filing_entries if start_date <= entry.filing_date <= end_date
    ]
    filing_entries.sort(key=lambda entry: (entry.filing_date, entry.path))
    if max_filings is not None:
        filing_entries = filing_entries[:max_filings]

    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    processed_filings = 0
    failed_filings: list[str] = []

    for entry in filing_entries:
        try:
            filing_rows = fetch_filing_transactions(entry, user_agent, ticker_filter)
            processed_filings += 1
            for row in filing_rows:
                key = (
                    row["source_url"],
                    row["insider_id"],
                    row["transaction_date"],
                    row["shares"],
                    row["price"],
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
        except Exception:
            failed_filings.append(entry.path)
        time.sleep(pause_seconds)

    rows.sort(key=lambda row: (row["ticker"], row["filing_date"], row["insider_id"]))
    fieldnames = [
        "filing_date",
        "transaction_date",
        "ticker",
        "issuer_name",
        "issuer_cik",
        "insider_id",
        "insider_name",
        "insider_role",
        "transaction_code",
        "transaction_type",
        "security_type",
        "shares",
        "price",
        "total_value",
        "source_url",
    ]
    write_csv(output_csv, rows, fieldnames)
    return {
        "output_csv": output_csv,
        "row_count": len(rows),
        "processed_filing_count": processed_filings,
        "failed_filing_count": len(failed_filings),
        "failed_filing_paths": failed_filings[:25],
        "unique_tickers": sorted({row["ticker"] for row in rows}),
    }


def fetch_yahoo_history(ticker: str, start_date: date, end_date: date) -> list[dict[str, str]]:
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
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))

    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        return []

    timestamps = result.get("timestamp") or []
    quotes = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quotes.get("open") or []
    highs = quotes.get("high") or []
    lows = quotes.get("low") or []
    closes = quotes.get("close") or []
    volumes = quotes.get("volume") or []

    rows: list[dict[str, str]] = []
    for index, timestamp in enumerate(timestamps):
        if index >= len(closes) or closes[index] is None:
            continue
        trading_date = datetime.fromtimestamp(timestamp, UTC).date()
        rows.append(
            {
                "date": trading_date.isoformat(),
                "open": "" if index >= len(opens) or opens[index] is None else str(opens[index]),
                "high": "" if index >= len(highs) or highs[index] is None else str(highs[index]),
                "low": "" if index >= len(lows) or lows[index] is None else str(lows[index]),
                "close": str(closes[index]),
                "volume": "" if index >= len(volumes) or volumes[index] is None else str(volumes[index]),
            }
        )
    return rows


def download_price_history(
    *,
    tickers: set[str],
    benchmark_ticker: str,
    start_date: date,
    end_date: date,
    output_csv: Path,
    lookback_padding_days: int = 60,
    forward_padding_days: int = 400,
    pause_seconds: float = 0.1,
) -> dict[str, object]:
    padded_start = start_date - timedelta(days=lookback_padding_days)
    padded_end = end_date + timedelta(days=forward_padding_days)

    all_rows: list[dict[str, str]] = []
    downloaded: list[str] = []
    missing: list[str] = []

    for ticker in sorted(set(tickers) | {normalize_ticker(benchmark_ticker)}):
        try:
            rows = fetch_yahoo_history(ticker, padded_start, padded_end)
            if not rows:
                missing.append(ticker)
                continue
            kept_any = False
            for row in rows:
                trading_date = parse_date(row.get("date", ""))
                if trading_date < padded_start or trading_date > padded_end:
                    continue
                all_rows.append(
                    {
                        "ticker": normalize_ticker(ticker),
                        "date": trading_date.isoformat(),
                        "open": row.get("open", ""),
                        "high": row.get("high", ""),
                        "low": row.get("low", ""),
                        "close": row.get("close", ""),
                        "volume": row.get("volume", ""),
                        "market_cap": "",
                    }
                )
                kept_any = True
            if kept_any:
                downloaded.append(ticker)
            else:
                missing.append(ticker)
        except urllib.error.HTTPError:
            missing.append(ticker)
        time.sleep(pause_seconds)

    all_rows.sort(key=lambda row: (row["ticker"], row["date"]))
    fieldnames = ["ticker", "date", "open", "high", "low", "close", "volume", "market_cap"]
    write_csv(output_csv, all_rows, fieldnames)
    return {
        "output_csv": output_csv,
        "row_count": len(all_rows),
        "downloaded_tickers": downloaded,
        "missing_tickers": missing,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, help="Inclusive start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="Inclusive end date in YYYY-MM-DD format.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory where CSVs will be written.")
    parser.add_argument(
        "--user-agent",
        required=True,
        help="SEC-compliant User-Agent string, for example 'Alex Christensen alex@example.com'.",
    )
    parser.add_argument(
        "--tickers",
        default="",
        help="Optional comma-separated ticker filter. Leave blank to pull all Form 4 filings in range.",
    )
    parser.add_argument(
        "--max-filings",
        type=int,
        default=250,
        help="Maximum number of Form 4 filings to fetch. Use a higher value for larger studies.",
    )
    parser.add_argument(
        "--benchmark",
        default="SPY",
        help="Benchmark ticker to include in the price download. Default: SPY.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    ticker_filter = parse_ticker_filter(args.tickers)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    insider_csv = args.output_dir / "insider_transactions.csv"
    prices_csv = args.output_dir / "daily_prices.csv"

    insider_result = download_form4_transactions(
        start_date=start_date,
        end_date=end_date,
        output_csv=insider_csv,
        user_agent=args.user_agent,
        ticker_filter=ticker_filter,
        max_filings=args.max_filings,
    )
    price_result = download_price_history(
        tickers=set(insider_result["unique_tickers"]),
        benchmark_ticker=args.benchmark,
        start_date=start_date,
        end_date=end_date,
        output_csv=prices_csv,
    )

    print("Downloaded raw study inputs")
    print(f"Insider rows: {insider_result['row_count']}")
    print(f"Processed SEC filings: {insider_result['processed_filing_count']}")
    print(f"Failed SEC filings: {insider_result['failed_filing_count']}")
    print(f"Unique tickers: {len(insider_result['unique_tickers'])}")
    print(f"Price rows: {price_result['row_count']}")
    print(f"Price tickers downloaded: {len(price_result['downloaded_tickers'])}")
    if price_result["missing_tickers"]:
        print(f"Missing price tickers: {', '.join(price_result['missing_tickers'][:20])}")
    print(f"Insider CSV: {insider_csv}")
    print(f"Prices CSV: {prices_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
