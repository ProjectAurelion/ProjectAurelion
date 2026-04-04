# Insider Event Study Tool

This directory now contains three connected pieces for the V1 insider-transaction event study described in [notes.md](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/notes.md):

* [insider_event_study.py](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/insider_event_study.py): the core backtest engine
* [data_pipeline.py](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/data_pipeline.py): a downloader for SEC Form 4 data and daily prices
* [dashboard_server.py](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/dashboard_server.py): a lightweight local UI for trying different rules and inspecting results

The stack is intentionally dependency-free. It uses the Python standard library plus public data endpoints.

## Fastest Way To Use It

Run the local dashboard:

```bash
python3 /Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool/dashboard_server.py
```

Then open `http://127.0.0.1:8765` in your browser.

From there you can:

* fetch real SEC Form 4 filings
* pull daily prices automatically
* adjust the cluster and tradability rules
* review summary tables, segment results, and top events
* open the generated CSV and Markdown output files

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
* `results_summary.csv`
* `segmented_analysis.csv`
* `summary.md`

The CLI now also prints a compact terminal summary so you can see candidate count, qualified-event count, and horizon-level performance immediately after a run.

## Notes

This is a strong V1 foundation, but it still inherits the limits of the source CSVs:

* SEC Form 4 parsing is focused on non-derivative transactions with transaction code `P`
* free price history may be weak for delisted tickers
* Yahoo chart price coverage is practical for a V1, but not institutional-grade
* security-type filtering depends on the metadata present in the filing
* market-cap segmentation only works when `market_cap` is available in the price data, and the current automated price pull does not provide it

For serious hypothesis testing, the right workflow is:

1. Start with a narrow date range or a ticker subset and confirm the study mechanics.
2. Widen the filing range and rerun with larger samples.
3. Once the signal looks promising, consider upgrading the price and security-master data before trusting portfolio-level conclusions.
