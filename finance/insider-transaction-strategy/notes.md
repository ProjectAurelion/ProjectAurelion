# Insider Transaction Strategy V1

## Objective

The immediate goal of this project is to answer one question cleanly:

**Do clustered open-market insider purchases predict positive forward equity returns after the information becomes public?**

Version 1 is a signal-validation project, not a full trading system. The first phase should determine whether the effect is real using a bias-aware event study built on free or public data sources. Portfolio construction, position sizing, and live execution decisions should only be added after the signal survives this first validation pass.

## Working Hypothesis

The core hypothesis is that discretionary insider buying contains useful information about future company performance, and that the signal becomes stronger when:

* more than one insider buys within a short period
* total insider purchase value is meaningfully large
* the buying occurs in stocks that are still liquid enough to trade

V1 should test whether these clustered insider-buy events outperform a market benchmark over multiple forward holding horizons after the filings become public.

## Scope

This study is limited to:

* US-listed common stocks
* open-market purchase transactions only
* publicly available or free data sources
* event-level analysis using forward returns

This study explicitly excludes, for V1:

* sell signals
* stop losses or profit targets
* portfolio sizing rules
* overlap management between concurrent positions
* live screening or automation

## Two-Stage Research Workflow

The project should proceed in two stages.

### Stage 1: Signal Validation

Build a clean event dataset, measure forward returns, compare them to SPY, and determine whether the signal has enough evidence to justify deeper work.

### Stage 2: Portfolio Construction

Only after Stage 1 shows a credible edge should the project move into trade simulation, capital allocation, overlapping signals, and risk management. That work belongs later and should not influence the first-pass validation.

## Required Inputs

V1 requires two research datasets.

### Insider Transaction Table

Minimum fields:

* ticker
* insider identifier or insider name
* insider role
* transaction code or transaction type
* transaction date
* filing date
* shares purchased
* transaction price
* total transaction value

### Daily Price Table

Minimum fields:

* date
* open
* high
* low
* close
* volume

If available, include market cap or shares outstanding so results can be segmented by company size, but market-cap availability is not a blocker for V1.

## Signal Definition

Each event in the V1 dataset should represent a qualifying cluster of insider buying activity.

### Eligible Securities

Include:

* US-listed common stocks

Exclude when identifiable from the source data:

* ETFs
* funds
* preferred shares
* warrants
* rights
* ADR variants if instrument identity is ambiguous
* other obvious non-common-share instruments

### Eligible Transactions

Include:

* open-market purchases only

Exclude:

* sales
* option exercises
* stock grants
* automatic plan transactions
* gifts
* conversions
* any non-discretionary or non-open-market activity

### Event Timestamp

Use the **filing date** as the event date.

The trade entry proxy for return measurement is the first tradable market session after the public filing becomes available. V1 must not use transaction date as the trigger.

### Cluster Rules

Create a signal when, within a rolling 30 calendar day window for the same ticker:

* at least 2 distinct insiders have qualifying purchases
* aggregate qualifying purchase value is at least $100,000

Treat the event date as the filing date that first satisfies both conditions.

### Cooldown Rule

Apply a 90 calendar day cooldown per ticker after a signal is created.

This prevents one wave of insider buying from producing multiple highly overlapping events.

## Tradability Filters

Apply the following filters as of the event date:

* stock price above $5
* average daily dollar volume above $1,000,000 over the prior 20 trading days
* market cap above $100,000,000 if market cap is available from the chosen source

If market cap is not available in the initial data pipeline, do not drop observations for that reason. Instead, defer company-size segmentation until size data is added.

## Event Study Design

V1 should evaluate signal quality with forward-return measurement rather than a portfolio backtest.

### Entry Convention

For each qualifying signal:

* use the first tradable session after the filing date as the entry date
* use the market close on that session as the default entry price unless the chosen data workflow supports a different convention consistently across both the signal stock and SPY

The same convention must be used for the benchmark comparison.

### Forward Return Horizons

Compute forward returns at:

* 21 trading days
* 63 trading days
* 126 trading days
* 252 trading days

These correspond approximately to 1, 3, 6, and 12 months.

### Delisting Handling

If a stock delists or price history ends before a horizon completes:

* record the realized path through the last available price
* flag the event as incomplete for that horizon
* keep the observation visible in the dataset so survivorship limitations are explicit

V1 should not silently discard these events.

## Benchmark and Evaluation

Use SPY as the benchmark for all horizons.

For each horizon, report:

* raw forward return
* excess return versus SPY
* hit rate
* mean return
* median return
* return distribution spread

At minimum, distribution spread should make it easy to inspect downside and upside dispersion, such as standard deviation plus selected percentile cutoffs.

## Segmented Analysis

V1 should report both overall results and segmented results. At minimum, include:

* insider-count buckets
* aggregate-purchase-value buckets

If company-size data is available, also segment by company size bucket rather than using company size as an assumed filter for alpha.

## Bias Controls

The write-up and implementation should explicitly guard against the following issues.

### Look-Ahead Bias

Signals must be triggered using filing date, not transaction date.

### Survivorship Bias

The project should preserve failed or incomplete observations whenever possible and clearly note where public data sources may underrepresent delisted names.

### Liquidity Bias

Tradability filters should be enforced using data available at the event date, not with hindsight.

### Overfitting

V1 should avoid parameter tuning beyond the fixed rules defined in this note. If the signal appears promising, robustness checks can be added later across different time periods and threshold values.

## V1 Deliverables

The first completed research pass should produce:

* one event dataset with one row per qualified signal
* one results summary table for each return horizon
* one segmented analysis table by signal-strength bucket
* one short conclusion stating whether the signal is strong enough to justify portfolio-level backtesting

## Exit Criteria For V1

V1 is complete when the analysis can answer all of the following:

* How many qualified events exist under the fixed rules?
* Do those events outperform SPY on average over 1, 3, 6, and 12 month horizons?
* Is the effect concentrated in stronger signals, such as more insiders or larger aggregate purchase value?
* Are the results robust enough to justify building a portfolio backtest next?
