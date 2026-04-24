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

If you provide `--prices-input-csv`, the tool will normalize that file into the internal schema and use it instead of the public price downloader. If your external file does not include the benchmark ticker, the tool will supplement the benchmark from the public downloader so the event study can still run.

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
* `summary.md`

The CLI now also prints a compact terminal summary so you can see candidate count, qualified-event count, and horizon-level performance immediately after a run.

## Notes

This is a strong V1 foundation, but it still inherits the limits of the source CSVs:

* SEC Form 4 parsing is focused on non-derivative transactions with transaction code `P`
* free price history may be weak for delisted tickers
* Yahoo chart price coverage is practical for a V1, but not institutional-grade
* security-type filtering depends on the metadata present in the filing
* market-cap segmentation only works when `market_cap` is available in the price data, and the current automated price pull does not provide it

For higher-level research and more production-like runs, the cleanest upgrade path is:

1. Keep the SEC Form 4 normalization layer as the raw insider source of truth.
2. Replace or enrich `daily_prices.csv` with a better vendor-grade export that includes `market_cap`, `shares_outstanding`, `sector`, `industry`, and cleaner delisting coverage.
3. Use `parameter_matched_outcomes.csv` as the event ledger of every stock that met the rules, and `ticker_outcome_summary.csv` as the cleaner ticker-level scorecard.
4. If your vendor export uses custom columns, add a small mapping JSON instead of hand-editing the CSV.

For serious hypothesis testing, the right workflow is:

1. Start with a narrow date range or a ticker subset and confirm the study mechanics.
2. Widen the filing range and rerun with larger samples.
3. Once the signal looks promising, consider upgrading the price and security-master data before trusting portfolio-level conclusions.
