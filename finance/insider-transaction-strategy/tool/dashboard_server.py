#!/usr/bin/env python3
"""Run a lightweight local dashboard for the insider event study."""

from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

TOOL_DIR = Path(__file__).resolve().parent
UI_FILE = TOOL_DIR / "ui" / "dashboard.html"
RUNS_DIR = TOOL_DIR / "runs"

sys.path.insert(0, str(TOOL_DIR))

import data_pipeline  # noqa: E402
import insider_event_study  # noqa: E402


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip()).strip("-").lower()
    return slug or "run"


def top_events(event_rows: list[dict[str, str]], horizon: int = 63, limit: int = 25) -> list[dict[str, str]]:
    key = f"excess_return_{horizon}d"
    complete_key = f"complete_{horizon}d"
    filtered = [row for row in event_rows if row.get(complete_key) == "yes" and row.get(key)]
    filtered.sort(key=lambda row: float(row[key]), reverse=True)
    return filtered[:limit]


def build_response(
    *,
    run_dir: Path,
    insider_result: dict[str, object],
    price_result: dict[str, object],
    study_result: dict[str, object],
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
            "results_summary": f"/runs/{run_dir.name}/analysis/results_summary.csv",
            "segmented_analysis": f"/runs/{run_dir.name}/analysis/segmented_analysis.csv",
            "summary_md": f"/runs/{run_dir.name}/analysis/summary.md",
        },
        "download": {
            "insider_rows": insider_result["row_count"],
            "processed_filing_count": insider_result["processed_filing_count"],
            "failed_filing_count": insider_result["failed_filing_count"],
            "unique_ticker_count": len(insider_result["unique_tickers"]),
            "price_row_count": price_result["row_count"],
            "price_ticker_count": len(price_result["downloaded_tickers"]),
            "missing_price_tickers": price_result["missing_tickers"],
            "failed_filing_paths": insider_result["failed_filing_paths"],
        },
        "study": {
            "candidate_count": study_result["candidate_count"],
            "qualified_count": study_result["qualified_count"],
            "rejected_count": study_result["rejected_count"],
            "benchmark": study_result["benchmark"],
            "summary_rows": study_result["summary_rows"],
            "segment_rows": study_result["segment_rows"],
            "top_events_63d": top_events(study_result["event_rows"], 63),
        },
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
        payload = json.loads(self.rfile.read(length) or b"{}")
        try:
            response = self.run_study(payload)
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

    def run_study(self, payload: dict[str, object]) -> dict[str, object]:
        start_date = data_pipeline.parse_date(str(payload["start_date"]))
        end_date = data_pipeline.parse_date(str(payload["end_date"]))
        user_agent = str(payload["user_agent"]).strip()
        if not user_agent:
            raise ValueError("A SEC-compliant User-Agent is required.")

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

        insider_result = data_pipeline.download_form4_transactions(
            start_date=start_date,
            end_date=end_date,
            output_csv=data_dir / "insider_transactions.csv",
            user_agent=user_agent,
            ticker_filter=ticker_filter,
            max_filings=max_filings,
        )
        price_result = data_pipeline.download_price_history(
            tickers=set(insider_result["unique_tickers"]),
            benchmark_ticker=benchmark,
            start_date=start_date,
            end_date=end_date,
            output_csv=data_dir / "daily_prices.csv",
        )
        study_result = insider_event_study.run_study(
            insider_csv=data_dir / "insider_transactions.csv",
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
        )
        return build_response(
            run_dir=run_dir,
            insider_result=insider_result,
            price_result=price_result,
            study_result=study_result,
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
