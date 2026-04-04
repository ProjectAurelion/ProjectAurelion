from __future__ import annotations

import sys
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

from form4_normalization import mark_superseded_amendments, parse_form4_rows


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_multi_owner_filing_collapses_to_joint_owner_group() -> None:
    rows = parse_form4_rows(
        filing_date="2024-01-05",
        form_type="4",
        source_url="https://www.sec.gov/Archives/edgar/data/1234567/000123456724000001/test.txt",
        filing_text=(FIXTURES / "multi_owner_form4.txt").read_text(encoding="utf-8"),
        ticker_filter=set(),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["owner_group_id"].startswith("JOINT:")
    assert row["is_multi_owner_filing"] == "yes"
    assert "CEO" in row["canonical_role"]
    assert "Director" in row["canonical_role"]
    assert "multi_owner_joint_filing" in row["data_quality_flags"]
    assert row["eligible_for_signal"] == "yes"


def test_indirect_plan_trade_is_flagged_and_not_signal_eligible() -> None:
    rows = parse_form4_rows(
        filing_date="2024-02-01",
        form_type="4",
        source_url="https://www.sec.gov/Archives/edgar/data/7654321/000765432124000001/test.txt",
        filing_text=(FIXTURES / "indirect_plan_form4.txt").read_text(encoding="utf-8"),
        ticker_filter=set(),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["eligible_for_signal"] == "no"
    assert row["canonical_role"] == "CFO"
    assert "possible_plan_trade" in row["data_quality_flags"]
    assert "indirect_ownership" in row["data_quality_flags"]
    assert "missing_acceptance_datetime" in row["data_quality_flags"]


def test_amended_filing_supersedes_original_row() -> None:
    original = {
        "issuer_cik": "0001234567",
        "owner_group_id": "0001111111",
        "period_of_report": "2024-01-05",
        "transaction_table": "non_derivative",
        "transaction_date": "2024-01-05",
        "transaction_code": "P",
        "security_type": "Common Stock",
        "shares": "1000",
        "price": "10",
        "ownership_type": "D",
        "acceptance_datetime": "2024-01-05 18:30:00",
        "filing_date": "2024-01-05",
        "accession": "000123456724000001",
    }
    amendment = {**original, "accession": "000123456724000002", "acceptance_datetime": "2024-01-06 08:00:00"}
    kept, removed = mark_superseded_amendments([original, amendment])
    assert removed == 1
    assert kept == [amendment]
