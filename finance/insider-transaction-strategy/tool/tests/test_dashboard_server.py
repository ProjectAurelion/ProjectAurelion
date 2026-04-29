from __future__ import annotations

import sys
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOL_DIR))

import dashboard_server


def test_parse_multipart_form_extracts_fields_and_files() -> None:
    boundary = "----CodexBoundary"
    content_type = f"multipart/form-data; boundary={boundary}"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="run_name"\r\n\r\n'
        "upload-test\r\n"
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="insider_upload"; filename="insider.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
        "ticker,filing_date,total_value\nAAA,2024-01-03,100000\n"
        f"\r\n--{boundary}--\r\n"
    ).encode("utf-8")

    fields, files = dashboard_server.parse_multipart_form(content_type, body)

    assert fields["run_name"] == "upload-test"
    assert files["insider_upload"]["filename"] == "insider.csv"
    assert b"ticker,filing_date" in files["insider_upload"]["content"]
