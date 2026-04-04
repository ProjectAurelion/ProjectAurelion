#!/usr/bin/env python3
"""Normalize SEC Form 4 filings into transaction rows for research."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Iterable, Optional


PLAN_TERMS = ("10b5", "trading plan", "rule 10b5-1", "automatic")


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


def parse_acceptance_datetime(filing_text: str) -> Optional[datetime]:
    match = re.search(r"<ACCEPTANCE-DATETIME>(\d{14})", filing_text)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d%H%M%S")


def extract_accession_number(source_url: str) -> str:
    filename = source_url.rstrip("/").split("/")[-1]
    return filename.removesuffix(".txt")


def extract_form4_xml(filing_text: str) -> str:
    matches = re.findall(r"<XML>(.*?)</XML>", filing_text, flags=re.DOTALL | re.IGNORECASE)
    for match in matches:
        if "<ownershipdocument" in match.lower():
            return match.strip()
    if "<ownershipdocument" in filing_text.lower():
        lower_text = filing_text.lower()
        start = lower_text.find("<ownershipdocument")
        end = lower_text.rfind("</ownershipdocument>")
        if start != -1 and end != -1:
            end += len("</ownershipdocument>")
            return filing_text[start:end].strip()
    raise ValueError("Could not locate an ownershipDocument XML segment in the filing.")


def canonical_role(owner_relationship: Optional[ET.Element]) -> tuple[str, str]:
    if owner_relationship is None:
        return "Other", ""

    officer_title = nested_text(owner_relationship, "officerTitle")
    title_lower = officer_title.lower()

    if nested_text(owner_relationship, "isOfficer") == "1":
        if "chief executive" in title_lower or title_lower == "ceo":
            return "CEO", officer_title
        if "chief financial" in title_lower or title_lower == "cfo":
            return "CFO", officer_title
        if "chief operating" in title_lower or title_lower == "coo":
            return "COO", officer_title
        if "president" in title_lower:
            return "President", officer_title
        if "chair" in title_lower:
            return "Chair", officer_title
        return "Other Officer", officer_title

    if nested_text(owner_relationship, "isDirector") == "1":
        return "Director", officer_title
    if nested_text(owner_relationship, "isTenPercentOwner") == "1":
        return "10% Owner", officer_title
    if nested_text(owner_relationship, "isOther") == "1":
        return "Other", nested_text(owner_relationship, "otherText")
    return "Other", officer_title


def join_unique(values: Iterable[str]) -> str:
    ordered: list[str] = []
    for value in values:
        normalized = value.strip()
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    return "; ".join(ordered)


def normalize_reporting_owner(root: ET.Element) -> dict[str, str]:
    owners: list[dict[str, str]] = []
    for owner in descendants(root, "reportingOwner"):
        owner_name = nested_text(owner, "reportingOwnerId", "rptOwnerName")
        owner_cik = nested_text(owner, "reportingOwnerId", "rptOwnerCik")
        role, role_detail = canonical_role(first_child(owner, "reportingOwnerRelationship"))
        owners.append(
            {
                "owner_id": owner_cik or owner_name,
                "owner_name": owner_name,
                "canonical_role": role,
                "role_detail": role_detail,
            }
        )

    if not owners:
        return {
            "owner_group_id": "",
            "owner_group_name": "",
            "canonical_role": "Other",
            "role_detail": "",
            "reported_owner_count": "0",
            "is_multi_owner_filing": "no",
        }

    if len(owners) == 1:
        owner = owners[0]
        return {
            "owner_group_id": owner["owner_id"],
            "owner_group_name": owner["owner_name"],
            "canonical_role": owner["canonical_role"],
            "role_detail": owner["role_detail"],
            "reported_owner_count": "1",
            "is_multi_owner_filing": "no",
        }

    owner_ids = sorted(owner["owner_id"] for owner in owners if owner["owner_id"])
    return {
        "owner_group_id": "JOINT:" + "|".join(owner_ids),
        "owner_group_name": " / ".join(owner["owner_name"] for owner in owners if owner["owner_name"]),
        "canonical_role": join_unique(owner["canonical_role"] for owner in owners),
        "role_detail": join_unique(owner["role_detail"] for owner in owners),
        "reported_owner_count": str(len(owners)),
        "is_multi_owner_filing": "yes",
    }


def classify_transaction(
    *,
    table_name: str,
    transaction_code: str,
    acquired_disposed: str,
    ownership_type: str,
    remarks_text: str,
) -> tuple[str, str, str]:
    flags: list[str] = []
    code = transaction_code.upper()
    acquired = acquired_disposed.upper()
    ownership = ownership_type.upper()
    remarks_lower = remarks_text.lower()

    if any(term in remarks_lower for term in PLAN_TERMS):
        flags.append("possible_plan_trade")
    if ownership == "I":
        flags.append("indirect_ownership")
    elif ownership not in {"D", "I"}:
        flags.append("ownership_ambiguous")

    if table_name == "derivative":
        classification = "derivative_transaction"
        eligible = "no"
    elif code == "P" and acquired == "A":
        classification = "open_market_purchase"
        eligible = "yes" if ownership == "D" and "possible_plan_trade" not in flags else "no"
    elif code == "A":
        classification = "grant_or_award"
        eligible = "no"
    elif code == "M":
        classification = "option_exercise"
        eligible = "no"
    elif code == "G":
        classification = "gift"
        eligible = "no"
    elif code == "C":
        classification = "conversion"
        eligible = "no"
    elif code in {"S", "D"} or acquired == "D":
        classification = "sale_or_disposition"
        eligible = "no"
    else:
        classification = "other"
        eligible = "no"

    return classification, eligible, "; ".join(flags)


def supersession_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        row.get("issuer_cik", ""),
        row.get("owner_group_id", ""),
        row.get("period_of_report", ""),
        row.get("transaction_table", ""),
        row.get("transaction_date", ""),
        row.get("transaction_code", ""),
        row.get("security_type", ""),
        row.get("shares", ""),
        row.get("price", ""),
        row.get("ownership_type", ""),
    )


def choose_latest_filing(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], str]:
    latest: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    for row in rows:
        key = supersession_key(row)
        candidate = (
            row.get("acceptance_datetime", ""),
            row.get("filing_date", ""),
            row.get("accession", ""),
        )
        current = latest.get(key)
        if current is None or candidate > current:
            latest[key] = candidate
    return {key: value[2] for key, value in latest.items()}


def mark_superseded_amendments(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    latest_accessions = choose_latest_filing(rows)
    kept_rows: list[dict[str, str]] = []
    removed_count = 0
    for row in rows:
        key = supersession_key(row)
        if latest_accessions.get(key) != row.get("accession", ""):
            removed_count += 1
            continue
        kept_rows.append(row)
    return kept_rows, removed_count


def parse_form4_rows(
    *,
    filing_date: str,
    form_type: str,
    source_url: str,
    filing_text: str,
    ticker_filter: set[str],
) -> list[dict[str, str]]:
    xml_text = extract_form4_xml(filing_text)
    root = ET.fromstring(xml_text)
    acceptance_datetime = parse_acceptance_datetime(filing_text)
    ticker = nested_text(root, "issuer", "issuerTradingSymbol").upper()
    if not ticker:
        return []
    if ticker_filter and ticker not in ticker_filter:
        return []

    issuer_name = nested_text(root, "issuer", "issuerName")
    issuer_cik = nested_text(root, "issuer", "issuerCik")
    period_of_report = nested_text(root, "periodOfReport")
    accession = extract_accession_number(source_url)
    owner_meta = normalize_reporting_owner(root)
    remarks_text = nested_text(root, "remarks")
    filing_dt = datetime.strptime(filing_date, "%Y-%m-%d").date()
    period_dt = datetime.strptime(period_of_report, "%Y-%m-%d").date() if period_of_report else None
    filing_lag_days = str((filing_dt - period_dt).days) if period_dt is not None else ""

    rows: list[dict[str, str]] = []
    for table_name, node_name in (("non_derivative", "nonDerivativeTransaction"), ("derivative", "derivativeTransaction")):
        for transaction in descendants(root, node_name):
            transaction_code = nested_text(transaction, "transactionCoding", "transactionCode").upper()
            acquired_disposed = nested_text(
                transaction,
                "transactionAmounts",
                "transactionAcquiredDisposedCode",
                "value",
            ).upper()
            security_title = nested_text(transaction, "securityTitle", "value")
            transaction_date = nested_text(transaction, "transactionDate", "value")
            shares = nested_text(transaction, "transactionAmounts", "transactionShares", "value")
            price = nested_text(transaction, "transactionAmounts", "transactionPricePerShare", "value")
            ownership_type = nested_text(
                transaction,
                "ownershipNature",
                "directOrIndirectOwnership",
                "value",
            ).upper()

            shares_float = float(shares) if shares else None
            price_float = float(price) if price else None
            total_value = shares_float * price_float if shares_float is not None and price_float is not None else None
            classification, eligible, flags = classify_transaction(
                table_name=table_name,
                transaction_code=transaction_code,
                acquired_disposed=acquired_disposed,
                ownership_type=ownership_type,
                remarks_text=remarks_text,
            )
            row_flags = [flag for flag in flags.split("; ") if flag]
            if owner_meta["is_multi_owner_filing"] == "yes":
                row_flags.append("multi_owner_joint_filing")
            if not acceptance_datetime:
                row_flags.append("missing_acceptance_datetime")

            rows.append(
                {
                    "accession": accession,
                    "form_type": form_type,
                    "is_amendment": "yes" if form_type.upper() == "4/A" else "no",
                    "filing_date": filing_date,
                    "acceptance_datetime": acceptance_datetime.isoformat(sep=" ") if acceptance_datetime else "",
                    "period_of_report": period_of_report,
                    "filing_lag_days": filing_lag_days,
                    "transaction_date": transaction_date,
                    "ticker": ticker,
                    "issuer_name": issuer_name,
                    "issuer_cik": issuer_cik,
                    "owner_group_id": owner_meta["owner_group_id"],
                    "owner_group_name": owner_meta["owner_group_name"],
                    "reported_owner_count": owner_meta["reported_owner_count"],
                    "is_multi_owner_filing": owner_meta["is_multi_owner_filing"],
                    "canonical_role": owner_meta["canonical_role"],
                    "role_detail": owner_meta["role_detail"],
                    "transaction_table": table_name,
                    "transaction_code": transaction_code,
                    "acquired_disposed_code": acquired_disposed,
                    "transaction_classification": classification,
                    "eligible_for_signal": eligible,
                    "security_type": security_title,
                    "ownership_type": ownership_type,
                    "is_direct_ownership": "yes" if ownership_type == "D" else "no" if ownership_type == "I" else "",
                    "shares": shares,
                    "price": price,
                    "total_value": f"{total_value:.6f}" if total_value is not None else "",
                    "data_quality_flags": "; ".join(dict.fromkeys(row_flags)),
                    "source_url": source_url,
                }
            )
    return rows
