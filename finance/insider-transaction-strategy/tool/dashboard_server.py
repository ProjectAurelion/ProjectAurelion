#!/usr/bin/env python3
"""Run a lightweight local dashboard for the insider event study."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import traceback
from datetime import datetime
from email.parser import BytesParser
from email.policy import default
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

TOOL_DIR = Path(__file__).resolve().parent
UI_FILE = TOOL_DIR / "ui" / "dashboard.html"
RUNS_DIR = TOOL_DIR / "runs"

sys.path.insert(0, str(TOOL_DIR))

import data_pipeline  # noqa: E402
import input_validation  # noqa: E402
import insider_event_study  # noqa: E402


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip()).strip("-").lower()
    return slug or "run"


def top_events(event_rows: list[dict[str, str]], horizon: int = 63, limit: int = 25) -> list[dict[str, str]]:
    key = f"net_bhar_return_{horizon}d"
    complete_key = f"complete_{horizon}d"
    filtered = [row for row in event_rows if row.get(complete_key) == "yes" and row.get(key)]
    filtered.sort(key=lambda row: float(row[key]), reverse=True)
    return filtered[:limit]


def bottom_events(event_rows: list[dict[str, str]], horizon: int = 63, limit: int = 25) -> list[dict[str, str]]:
    key = f"net_bhar_return_{horizon}d"
    complete_key = f"complete_{horizon}d"
    filtered = [row for row in event_rows if row.get(complete_key) == "yes" and row.get(key)]
    filtered.sort(key=lambda row: float(row[key]))
    return filtered[:limit]


def parse_multipart_form(content_type: str, body: bytes) -> tuple[dict[str, str], dict[str, dict[str, object]]]:
    header_block = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8")
    message = BytesParser(policy=default).parsebytes(header_block + body)
    fields: dict[str, str] = {}
    files: dict[str, dict[str, object]] = {}

    for part in message.iter_parts():
        if part.get_content_disposition() != "form-data":
            continue
        name = part.get_param("name", header="content-disposition")
        if not name:
            continue
        filename = part.get_filename()
        payload = part.get_payload(decode=True) or b""
        if filename:
            files[name] = {
                "filename": filename,
                "content": payload,
            }
        else:
            charset = part.get_content_charset() or "utf-8"
            fields[name] = payload.decode(charset, errors="ignore")
    return fields, files


def write_uploaded_file(upload: dict[str, object], destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(upload["content"])
    return destination


def build_response(
    *,
    run_dir: Path,
    insider_result: dict[str, object],
    price_result: dict[str, object],
    study_result: dict[str, object],
    preflight: dict[str, object],
) -> dict[str, object]:
    return {
        "run_dir": str(run_dir),
        "data_dir": str(run_dir / "data"),
        "analysis_dir": str(run_dir / "analysis"),
        "files": {
            "insider_csv": f"/runs/{run_dir.name}/data/insider_transactions.csv",
            "prices_csv": f"/runs/{run_dir.name}/data/daily_prices.csv",
            "signal_candidates": f"/runs/{run_dir.name}/analysis/signal_candidates.csv",
            "qualified_events": f"/runs/{run_dir.name}/analysis/qualified_events.csv",
            "parameter_matched_outcomes": f"/runs/{run_dir.name}/analysis/parameter_matched_outcomes.csv",
            "ticker_outcome_summary": f"/runs/{run_dir.name}/analysis/ticker_outcome_summary.csv",
            "results_summary": f"/runs/{run_dir.name}/analysis/results_summary.csv",
            "segmented_analysis": f"/runs/{run_dir.name}/analysis/segmented_analysis.csv",
            "research_summary_json": f"/runs/{run_dir.name}/analysis/research_summary.json",
            "case_study_summary_json": f"/runs/{run_dir.name}/analysis/case_study_summary.json",
            "case_study_md": f"/runs/{run_dir.name}/analysis/case_study.md",
            "summary_md": f"/runs/{run_dir.name}/analysis/summary.md",
        },
        "download": {
            "insider_input_mode": insider_result.get("input_mode", "sec_download"),
            "insider_input_file": insider_result.get("input_file", ""),
            "insider_rows": insider_result["row_count"],
            "raw_insider_rows": insider_result["raw_row_count"],
            "superseded_rows_removed": insider_result["superseded_row_count"],
            "processed_filing_count": insider_result["processed_filing_count"],
            "failed_filing_count": insider_result["failed_filing_count"],
            "unique_ticker_count": len(insider_result["unique_tickers"]),
            "unique_issuer_count": len(insider_result["unique_issuers"]),
            "price_row_count": price_result["row_count"],
            "price_ticker_count": len(price_result["downloaded_tickers"]),
            "missing_price_tickers": price_result["missing_tickers"],
            "adjusted_price_row_count": price_result["adjusted_price_row_count"],
            "market_cap_row_count": price_result["market_cap_row_count"],
            "price_input_mode": price_result.get("input_mode", "public_download"),
            "price_input_csv": price_result.get("input_csv", ""),
            "price_input_file": price_result.get("input_file", ""),
            "benchmark_supplemented": price_result.get("benchmark_supplemented", "no"),
            "price_vendor_profile": price_result.get("price_vendor_profile", ""),
            "price_vendor_description": price_result.get("price_vendor_description", ""),
            "price_mapping_json": price_result.get("price_mapping_json", ""),
            "market_data_provider": price_result.get("market_data_provider", "yahoo_public"),
            "market_data_base_url": price_result.get("market_data_base_url", ""),
            "skipped_row_count": price_result.get("skipped_row_count", 0),
            "duplicate_row_count": price_result.get("duplicate_row_count", 0),
            "derived_market_cap_row_count": price_result.get("derived_market_cap_row_count", 0),
            "derived_adjusted_ohlc_row_count": price_result.get("derived_adjusted_ohlc_row_count", 0),
            "profile_enriched_ticker_count": price_result.get("profile_enriched_ticker_count", 0),
            "historical_market_cap_ticker_count": price_result.get("historical_market_cap_ticker_count", 0),
            "field_coverage_pct": price_result.get("field_coverage_pct", {}),
            "failed_filing_paths": insider_result["failed_filing_paths"],
            "cache_dir": insider_result["cache_dir"],
        },
        "preflight": preflight,
        "study": {
            "candidate_count": study_result["candidate_count"],
            "raw_qualified_count": study_result["qualified_raw_count"],
            "qualified_count": study_result["qualified_count"],
            "investable_count": study_result["coverage"]["primary_investable_event_count"],
            "rejected_count": study_result["rejected_count"],
            "benchmark": study_result["benchmark"],
            "summary_rows": study_result["summary_rows"],
            "segment_rows": study_result["segment_rows"],
            "event_rows": study_result["event_rows"],
            "parameter_outcome_rows": study_result["parameter_outcome_rows"],
            "ticker_summary_rows": study_result["ticker_summary_rows"],
            "research_summary": study_result["research_summary"],
            "case_study": study_result["case_study"],
            "top_events_63d": top_events(study_result["event_rows"], 63),
            "bottom_events_63d": bottom_events(study_result["event_rows"], 63),
        },
        "warnings": study_result["warnings"],
        "coverage": study_result["coverage"],
        "methodology": study_result["methodology"],
        "console_summary": insider_event_study.build_console_summary(study_result),
    }


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html"}:
            self.serve_file(UI_FILE, "text/html; charset=utf-8")
            return

        if parsed.path.startswith("/runs/"):
            relative = parsed.path.removeprefix("/runs/")
            target = (RUNS_DIR / unquote(relative)).resolve()
            if RUNS_DIR.resolve() not in target.parents and target != RUNS_DIR.resolve():
                self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
                return
            if not target.exists() or not target.is_file():
                self.send_error(HTTPStatus.NOT_FOUND, "File not found")
                return
            content_type = "text/plain; charset=utf-8"
            if target.suffix == ".csv":
                content_type = "text/csv; charset=utf-8"
            elif target.suffix == ".md":
                content_type = "text/markdown; charset=utf-8"
            self.serve_file(target, content_type)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:
        if self.path != "/api/run":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length) or b""
        content_type = self.headers.get("Content-Type", "")
        if content_type.startswith("multipart/form-data"):
            payload, files = parse_multipart_form(content_type, raw_body)
        else:
            payload = json.loads(raw_body or b"{}")
            files = {}
        try:
            response = self.run_study(payload, files)
            self.send_json(HTTPStatus.OK, response)
        except Exception as exc:
            self.send_json(
                HTTPStatus.BAD_REQUEST,
                {
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )

    def log_message(self, format: str, *args: object) -> None:
        return

    def serve_file(self, path: Path, content_type: str) -> None:
        data = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def run_study(self, payload: dict[str, object], files: dict[str, dict[str, object]]) -> dict[str, object]:
        start_date = data_pipeline.parse_date(str(payload["start_date"]))
        end_date = data_pipeline.parse_date(str(payload["end_date"]))
        user_agent = str(payload["user_agent"]).strip()

        run_name = slugify(str(payload.get("run_name", "")).strip() or "study")
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        run_dir = RUNS_DIR / f"{timestamp}-{run_name}"
        data_dir = run_dir / "data"
        analysis_dir = run_dir / "analysis"
        data_dir.mkdir(parents=True, exist_ok=True)
        analysis_dir.mkdir(parents=True, exist_ok=True)

        ticker_filter = data_pipeline.parse_ticker_filter(str(payload.get("tickers", "")))
        benchmark = str(payload.get("benchmark", "SPY")).strip() or "SPY"
        max_filings = int(payload.get("max_filings", 250) or 250)
        external_prices_csv = str(payload.get("external_prices_csv", "")).strip()
        external_prices_vendor_profile = str(payload.get("external_prices_vendor_profile", "generic") or "generic").strip()
        external_prices_mapping_json = str(payload.get("external_prices_mapping_json", "")).strip()
        market_data_provider = str(payload.get("market_data_provider", "yahoo_public") or "yahoo_public").strip()
        market_data_api_key = str(
            payload.get("market_data_api_key", "") or os.getenv("MARKET_DATA_API_KEY", os.getenv("FMP_API_KEY", ""))
        ).strip()
        market_data_base_url = str(
            payload.get("market_data_base_url", data_pipeline.FMP_STABLE_BASE_URL) or data_pipeline.FMP_STABLE_BASE_URL
        ).strip()
        insider_upload = files.get("insider_upload")
        prices_upload = files.get("prices_upload")
        mapping_upload = files.get("mapping_upload")

        if not user_agent and insider_upload is None:
            raise ValueError("A SEC-compliant User-Agent is required when downloading filings from the SEC.")

        if insider_upload is not None:
            insider_path = write_uploaded_file(insider_upload, data_dir / "insider_transactions.csv")
            insider_check = input_validation.validate_insider_csv(insider_path)
            insider_result = {
                "output_csv": insider_path,
                "row_count": insider_check["usable_row_count"],
                "raw_row_count": insider_check["row_count"],
                "superseded_row_count": 0,
                "processed_filing_count": 0,
                "failed_filing_count": 0,
                "failed_filing_paths": [],
                "unique_tickers": insider_check["unique_tickers"],
                "unique_issuers": insider_check["unique_issuers"],
                "cache_dir": str(data_pipeline.CACHE_ROOT),
                "input_mode": "uploaded_csv",
                "input_file": insider_upload["filename"],
            }
        else:
            insider_result = data_pipeline.download_form4_transactions(
                start_date=start_date,
                end_date=end_date,
                output_csv=data_dir / "insider_transactions.csv",
                user_agent=user_agent,
                ticker_filter=ticker_filter,
                max_filings=max_filings,
            )
            insider_path = data_dir / "insider_transactions.csv"

        if mapping_upload is not None:
            mapping_json_path: Path | None = write_uploaded_file(mapping_upload, data_dir / "price_mapping.json")
        elif external_prices_mapping_json:
            mapping_json_path = Path(external_prices_mapping_json).expanduser()
        else:
            mapping_json_path = None

        if prices_upload is not None:
            uploaded_prices_source = write_uploaded_file(prices_upload, data_dir / "uploaded_prices_source.csv")
            price_result = data_pipeline.normalize_external_price_history(
                input_csv=uploaded_prices_source,
                benchmark_ticker=benchmark,
                start_date=start_date,
                end_date=end_date,
                output_csv=data_dir / "daily_prices.csv",
                vendor_profile=external_prices_vendor_profile,
                mapping_json=mapping_json_path,
            )
            price_result["input_mode"] = "uploaded_csv"
            price_result["input_file"] = prices_upload["filename"]
        elif external_prices_csv:
            price_result = data_pipeline.normalize_external_price_history(
                input_csv=Path(external_prices_csv).expanduser(),
                benchmark_ticker=benchmark,
                start_date=start_date,
                end_date=end_date,
                output_csv=data_dir / "daily_prices.csv",
                vendor_profile=external_prices_vendor_profile,
                mapping_json=mapping_json_path,
            )
            price_result["input_mode"] = "external_csv_path"
        else:
            price_result = data_pipeline.download_price_history(
                tickers=set(insider_result["unique_tickers"]),
                benchmark_ticker=benchmark,
                start_date=start_date,
                end_date=end_date,
                output_csv=data_dir / "daily_prices.csv",
                market_data_provider=market_data_provider,
                market_data_api_key=market_data_api_key,
                market_data_base_url=market_data_base_url,
            )

        preflight = input_validation.build_preflight_summary(
            insider_csv=insider_path,
            prices_csv=data_dir / "daily_prices.csv",
            benchmark_ticker=benchmark,
            insider_input_mode=str(insider_result.get("input_mode", "sec_download")),
            price_input_mode=str(price_result.get("input_mode", "public_download")),
        )
        if not preflight["ready"]:
            raise ValueError(
                "Input check failed:\n- " + "\n- ".join(preflight["errors"])
            )
        study_result = insider_event_study.run_study(
            insider_csv=insider_path,
            prices_csv=data_dir / "daily_prices.csv",
            output_dir=analysis_dir,
            benchmark=benchmark,
            window_days=int(payload.get("window_days", 30) or 30),
            cooldown_days=int(payload.get("cooldown_days", 90) or 90),
            min_distinct_insiders=int(payload.get("min_distinct_insiders", 2) or 2),
            min_total_value=float(payload.get("min_total_value", 100000.0) or 100000.0),
            min_price=float(payload.get("min_price", 5.0) or 5.0),
            min_daily_dollar_volume=float(payload.get("min_daily_dollar_volume", 1000000.0) or 1000000.0),
            lookback_days=int(payload.get("lookback_days", 20) or 20),
            min_market_cap=float(payload.get("min_market_cap", 100000000.0) or 100000000.0),
            entry_timing=str(payload.get("entry_timing", "next_session_close") or "next_session_close"),
            commission_bps_per_side=float(payload.get("commission_bps_per_side", 0.0) or 0.0),
            slippage_bps_per_side=float(payload.get("slippage_bps_per_side", 10.0) or 10.0),
            assumed_position_size=float(payload.get("assumed_position_size", 50000.0) or 50000.0),
            max_adv_participation=float(payload.get("max_adv_participation", 0.1) or 0.1),
            microcap_cutoff=float(payload.get("microcap_cutoff", 300000000.0) or 300000000.0),
        )
        return build_response(
            run_dir=run_dir,
            insider_result=insider_result,
            price_result=price_result,
            study_result=study_result,
            preflight=preflight,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind. Default: 8765")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Dashboard running at http://{args.host}:{args.port}")
    print("Use the browser UI to fetch SEC Form 4 data, run the study, and inspect the results.")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
