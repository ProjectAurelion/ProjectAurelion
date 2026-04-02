# Portfolio Backtests Deferred Until After V1 Validation

## Current Status

Portfolio-level backtesting is intentionally out of scope for the first phase of this project.

The immediate priority is to validate whether clustered open-market insider purchases have predictive power in a clean event study. Until that evidence exists, adding stop losses, position sizing, rebalancing, or capital-allocation logic would mix research questions and make the result harder to interpret.

## Why Backtests Are Deferred

The V1 event study is designed to isolate signal quality. A portfolio backtest adds additional decisions that can hide whether the edge is coming from the signal itself or from portfolio construction choices.

Those deferred choices include:

* position sizing
* maximum concurrent positions
* sector concentration limits
* overlap handling for multiple active signals
* rebalance cadence
* stop-loss logic
* profit targets
* trailing exits
* cash management
* slippage and transaction-cost modeling

## Gate To Start Portfolio Backtesting

Portfolio simulation should only begin after V1 answers all of the following with credible evidence:

* the event sample size is large enough to analyze
* forward returns are meaningfully positive versus SPY
* excess returns persist across more than one holding horizon
* stronger signal buckets look better than weaker ones
* the result still appears interesting after considering data-quality and survivorship limitations

## Planned Scope For A Later Backtest Phase

If V1 passes the gate, this file should be expanded into a full portfolio-spec document covering:

* portfolio construction rules
* entry and exit execution assumptions
* overlapping-position handling
* sizing model
* turnover expectations
* transaction-cost assumptions
* drawdown analysis
* benchmark comparison at the portfolio level

## Minimum Future Outputs

When this phase begins, the backtesting workflow should eventually produce:

* trade log
* portfolio equity curve
* drawdown series
* benchmark-relative performance summary
* sensitivity analysis across major strategy parameters
