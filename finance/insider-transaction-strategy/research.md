# Research Notes For Insider Transaction Strategy V1

## Purpose

This file captures supporting research decisions for the V1 event study. The goal is to keep `notes.md` focused on the actual study design while this document tracks source quality, transaction definitions, and known limitations.

## Data Source Strategy

V1 should start with free or public data that is easy to access and replace later if the signal looks promising.

### Insider Data

Practical starting points:

* SEC Form 4 data, directly or through a public aggregator
* OpenInsider-style datasets if they provide the required fields cleanly
* Finviz-style pages only if the data can be captured consistently and legally

Required insider-data fields for V1:

* ticker
* insider identifier or name
* insider role
* transaction code or transaction type
* transaction date
* filing date
* shares
* price
* total transaction value

### Price Data

Practical starting point:

* Yahoo-style daily OHLCV history, such as `yfinance`, for signal stocks and SPY

Required price-data fields:

* date
* open
* high
* low
* close
* volume

Helpful but optional:

* shares outstanding
* market cap

## Transaction-Type Definitions

V1 should only include discretionary insider buying that most closely reflects a deliberate capital allocation decision by the insider.

### Include

* open-market purchases

### Exclude

* open-market sales
* option exercises
* derivative conversions
* restricted stock grants
* gifts
* transactions under automatic trading plans
* acquisitions where the economics are not comparable to a direct common-share purchase

If the source encodes SEC transaction codes, document exactly which code or codes map to "open-market purchase" before running the study.

## Security Filtering

The study is intended for common stocks, not every tradable symbol that may appear in insider datasets.

When instrument metadata is available, exclude:

* ETFs
* mutual funds and closed-end funds
* preferred shares
* warrants and rights
* units
* special share classes that are not easily comparable to ordinary common stock

If security-type metadata is incomplete, track those rows separately rather than silently mixing them into the main study.

## Known Limitations Of Public Data

### Filing Normalization

Public aggregators may normalize filings differently, especially for insider identity, officer titles, or amended filings.

### Delisting Coverage

Free price sources and public scrapes may have weaker historical coverage for delisted or inactive symbols, which can bias long-horizon results upward if not handled carefully.

### Symbol Mapping

Ticker changes, mergers, and relistings may create mismatches between insider records and price histories.

### Amendments And Duplicates

Some filings may be amended or duplicated across sources. The data-cleaning layer should detect repeated records before constructing event clusters.

### Market Cap Availability

Market cap is useful for segmentation, but it may not be available historically for every event in a free pipeline. V1 should not depend on it for the core test.

## Recommended Data Checks

Before trusting the results, verify:

* filing dates are present and parseable
* transaction values are numeric and comparable
* duplicate filings are removed
* insider names or identifiers are normalized enough to count distinct insiders correctly
* price histories cover the entry date and the required forward horizons when possible
* SPY data uses the same calendar and pricing convention as the signal stocks

## Study Outputs To Preserve

The research workflow should preserve three durable outputs:

* a cleaned insider-transactions dataset
* a qualified event dataset after filters and cooldown rules
* a results package with overall and segmented forward-return summaries

Keeping those layers separate will make it easier to audit the study and replace inputs later with cleaner vendor data.

## Upgrade Path After V1

If the signal survives the first event study, the next improvements should be:

* better delisting-aware price history
* stronger security master and symbol mapping
* historical market-cap enrichment
* robustness checks across parameter variations
* portfolio-level simulation in `backtests.md`
