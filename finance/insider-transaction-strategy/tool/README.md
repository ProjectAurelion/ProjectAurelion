# Insider Event Study Tool

If you are trying to use the project rather than develop it, start from the top-level strategy folder:

```bash
cd finance/insider-transaction-strategy
./launch_dashboard.sh
```

The top-level [README.md](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/README.md) is the cleaner landing page. This file is the deeper technical reference for the tool itself.

This directory now contains three connected pieces for the V1 insider-transaction event study described in [notes.md](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/notes.md):

* [insider_event_study.py](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/insider_event_study.py): the core backtest engine
* [data_pipeline.py](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/data_pipeline.py): a downloader for SEC Form 4 data and daily prices
* [dashboard_server.py](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/dashboard_server.py): a lightweight local UI for trying different rules and inspecting results

The stack is intentionally dependency-light. It uses the Python standard library, the SEC as the insider source of truth, and optional API-backed market-data enrichment for more production-like runs.

## Fastest Way To Use It

For a friend cloning the repo fresh, the easiest path is:

```bash
cd finance/insider-transaction-strategy/tool
cp .env.example .env
# put your FMP API key in .env if you want the no-CSV workflow
./run_dashboard.sh
```

Then open `http://127.0.0.1:8876` in your browser.

You can also skip the `.env` step and paste the API key directly into the dashboard UI.

If you prefer to launch manually, run the local dashboard:

```bash
python3 /Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/dashboard_server.py
```

Then open `http://127.0.0.1:8765` or whichever port you chose in your browser.

From there you can:

* fetch real SEC Form 4 filings
* upload your own insider CSV and price/reference CSV directly from the browser
* pull daily prices automatically from either the public fallback or a richer API provider
* adjust the cluster and tradability rules
* run an input check to confirm the files were usable after normalization
* review summary tables, segment results, and top events
* open the generated CSV and Markdown output files

## Best Push-Button Workflow

If your goal is “tell me whether delayed Form 4 tracking looks tradable without making me source and normalize vendor CSVs by hand,” the cleanest path is now:

1. Leave the insider upload empty so the tool downloads official SEC Form 4 filings directly.
2. Set the market-data provider to `FMP API`.
3. Provide your FMP API key in the dashboard, or set `MARKET_DATA_API_KEY` / `FMP_API_KEY` in your environment.
4. Run the study and read:
   * `case_study.md` for the thesis-level answer
   * `parameter_matched_outcomes.csv` for the event ledger
   * `ticker_outcome_summary.csv` for the ticker scorecard

This lets the tool generate `daily_prices.csv` internally from the API, with richer reference fields such as market cap, shares outstanding, sector, industry, exchange, country, and source labels when available.

## Shareable GitHub Quickstart

If you want to send this to a friend and have them run it from GitHub:

1. Clone the repository.
2. Check out the branch that contains the tool if it is not merged to `main` yet.
3. Go to [tool](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool).
4. Run:

```bash
cp .env.example .env
./run_dashboard.sh
```

5. Open `http://127.0.0.1:8876`
6. Paste a real SEC User-Agent and either:
   * enter an FMP API key in the UI, or
   * store it in `.env`
7. Run the study

The startup script creates a virtual environment automatically, installs runtime dependencies, and launches the dashboard.

## CLI Modes

### 1. Download Real Inputs

```bash
python3 /Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/data_pipeline.py \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --output-dir /path/to/run/data \
  --user-agent "Alex Christensen alex@example.com"
```

Optional flags:

* `--tickers AAPL,MSFT,NVDA`
* `--max-filings 500`
* `--benchmark SPY`
* `--market-data-provider fmp_api`
* `--market-data-api-key YOUR_KEY`
* `--market-data-base-url https://financialmodelingprep.com/stable`
* `--prices-input-csv /path/to/vendor_prices.csv`
* `--prices-vendor-profile generic`
* `--prices-mapping-json /path/to/vendor_mapping.json`

This writes:

* `insider_transactions.csv`
* `daily_prices.csv`

### 2. Run The Study On CSV Inputs

```bash
python3 /Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/insider_event_study.py \
  --insider-csv /path/to/insider_transactions.csv \
  --prices-csv /path/to/daily_prices.csv \
  --output-dir /path/to/output
```

## Insider Transactions CSV

Expected columns:

* `ticker` or `symbol`
* `insider_id`, `insider_name`, or `reporting_owner_name`
* `insider_role`, `role`, or `title`
* `filing_date`
* `transaction_date`
* `transaction_code` or `transaction_type`
* `shares`
* `price`
* `total_value`

The tool only keeps rows that look like open-market purchases. If `transaction_code` is present, it currently treats SEC code `P` as eligible and excludes all other codes.

## Daily Prices CSV

Expected columns:

* `ticker` or `symbol`
* `date`
* `open`
* `high`
* `low`
* `close`
* `volume`
* optional `market_cap`

The file must include rows for both the signal stocks and the benchmark ticker, which defaults to `SPY`.

Optional richer research columns are also supported when you have better data:

* `shares_outstanding`
* `sector`
* `industry`
* `exchange`
* `country`
* `price_source`

If you provide `--prices-input-csv`, the tool will normalize that file into the internal schema and use it instead of the API/public price downloader. If your external file does not include the benchmark ticker, the tool will supplement the benchmark from the public downloader so the event study can still run.

If you do not provide `--prices-input-csv`, you can now choose the market-data source directly:

* `--market-data-provider yahoo_public`
  Good for quick tests, but weak on market cap and reference coverage.
* `--market-data-provider fmp_api`
  Better for production-like research. The tool will call the Financial Modeling Prep API directly, enrich the normalized price history with market cap, shares outstanding, sector, industry, exchange, country, and preserve source labels in `daily_prices.csv`.

The external price import now supports adapter profiles:

* `generic`
  A flexible default for common CSV exports with names like `Symbol`, `Date`, `Close`, `Volume`, `MarketCap`, and `Adjusted_Close`.
* `normalized`
  For files that already use the tool's canonical schema directly.
* `institutional`
  For richer exports with fields like `px_last`, `px_volume`, `cur_mkt_cap`, `shr_out`, `gics_sector_name`, and `gics_sub_industry_name`.

You can also provide `--prices-mapping-json` to override column mappings or set constants for a specific vendor export. Example:

```json
{
  "profile": "institutional",
  "fields": {
    "ticker": ["ric", "symbol"],
    "date": "as_of_date",
    "close": "close_px",
    "adj_close": "total_return_close"
  },
  "constants": {
    "price_source": "premium_vendor_export"
  }
}
```

When the external file includes `shares_outstanding` but not `market_cap`, the adapter will derive market cap from price times shares outstanding so the market-cap filter can still function more often.

## What The Tool Does

1. Filters insider rows down to open-market purchases in common-stock-like instruments.
2. Detects a signal when a ticker has at least 2 distinct insiders and at least $100,000 of aggregate insider buying inside a rolling 30-day window.
3. Applies a 90-day cooldown per ticker.
4. Uses the first tradable session after the filing date as the entry date.
5. Applies the V1 tradability filters:
   * entry price above $5
   * 20-day average daily dollar volume above $1,000,000
   * market cap above $100,000,000 when market cap is present
6. Computes forward returns at 21, 63, 126, and 252 trading days for both the signal stock and `SPY`.

## Outputs

The output directory will contain:

* `signal_candidates.csv`
* `qualified_events.csv`
* `parameter_matched_outcomes.csv`
* `ticker_outcome_summary.csv`
* `results_summary.csv`
* `segmented_analysis.csv`
* `research_summary.json`
* `case_study_summary.json`
* `case_study.md`
* `summary.md`

The CLI now also prints a compact terminal summary so you can see candidate count, qualified-event count, and horizon-level performance immediately after a run.

The case-study outputs are meant to answer the push-button question more directly:

* `case_study_summary.json`
  A machine-readable verdict on whether delayed Form 4 tracking looks weak, mixed, promising, or still too early to judge under the chosen rules.
* `case_study.md`
  A concise human-readable memo that frames the trading-thesis question, summarizes the evidence, and suggests next steps.

## Dashboard Upload Workflow

The dashboard now supports two ways to provide data:

1. Download mode
   * leave the upload fields empty
   * provide a SEC User-Agent
   * the tool will fetch Form 4 data and public price history

2. Upload mode
   * upload an insider CSV directly in the browser
   * optionally upload a richer prices/reference CSV
   * optionally upload a mapping JSON if your vendor columns need overrides

Uploaded files take priority over local path fields. If you upload an insider CSV, the dashboard skips the SEC download step. If you upload a price CSV, the dashboard skips the public price downloader and runs the vendor adapter path instead.

3. API mode
   * leave the price upload and local path fields empty
   * choose `FMP API` as the market-data provider
   * enter an API key or set `MARKET_DATA_API_KEY` / `FMP_API_KEY`

In API mode the tool keeps SEC Form 4 normalization as the raw insider source of truth, then generates `daily_prices.csv` internally from the API rather than asking you to prepare a vendor export first.

Every run now includes an **Input Check** section in the Results tab so you can confirm:

* whether the insider file had enough usable rows
* whether the price file had adjusted prices and market-cap coverage
* whether the benchmark was present
* whether insider tickers actually overlapped with the price dataset

## Notes

This is a strong V1 foundation, but it still inherits the limits of the source CSVs:

* SEC Form 4 parsing is focused on non-derivative transactions with transaction code `P`
* free price history may be weak for delisted tickers
* Yahoo chart price coverage is practical for a V1, but not institutional-grade
* security-type filtering depends on the metadata present in the filing
* market-cap segmentation only works when `market_cap` is available in the price data, and the current automated price pull does not provide it

For higher-level research and more production-like runs, the cleanest upgrade path is now:

1. Keep the SEC Form 4 normalization layer as the raw insider source of truth.
2. Prefer the direct API-backed market-data path so the tool can generate `daily_prices.csv` internally without forcing you to source and hand-normalize CSV files first.
3. If you already have premium vendor exports, use `--prices-input-csv` and an optional mapping JSON as a fallback path rather than editing raw files by hand.
4. Use `parameter_matched_outcomes.csv` as the event ledger of every stock that met the rules, and `ticker_outcome_summary.csv` as the cleaner ticker-level scorecard.

For serious hypothesis testing, the right workflow is:

1. Start with a narrow date range or a ticker subset and confirm the study mechanics.
2. Widen the filing range and rerun with larger samples.
3. Once the signal looks promising, consider upgrading the price and security-master data before trusting portfolio-level conclusions.
