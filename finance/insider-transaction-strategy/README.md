# Insider Transaction Strategy

This folder now has one main purpose:

**Run a case study to test whether delayed Form 4 insider-buy clusters appear to be a viable trading signal.**

If you are here to use the tool, start with the launcher below.

## Start Here

From the repository root:

```bash
cd finance/insider-transaction-strategy
./launch_dashboard.sh
```

Then open:

```text
http://127.0.0.1:8876
```

The launcher delegates to the actual app in [tool](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool), creates a local virtual environment if needed, installs runtime dependencies, and starts the dashboard.

## Fastest Friend Workflow

If someone clones the repo from GitHub, they should do:

```bash
git clone https://github.com/ProjectAurelion/ProjectAurelion.git
cd ProjectAurelion/finance/insider-transaction-strategy
./launch_dashboard.sh
```

Optional API key setup:

```bash
cp tool/.env.example tool/.env
```

Then put the FMP key in `tool/.env`, or paste it directly into the dashboard UI.

## What To Use In The UI

Recommended no-CSV workflow:

* leave insider upload empty
* leave prices/reference upload empty
* set `Market data provider` to `FMP API`
* enter your API key
* use your real SEC User-Agent
* run the study

Then read:

* `Case Study`
* `Quick Read`
* `Input Check`
* `parameter_matched_outcomes.csv`
* `ticker_outcome_summary.csv`

## Folder Layout

Only a few items matter for normal usage:

* [launch_dashboard.sh](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/launch_dashboard.sh): top-level launcher
* [tool](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/tool): the actual application
* [notes.md](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/notes.md): strategy specification
* [research.md](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/research.md): supporting research notes
* [backtests.md](/Users/alexchristensen/Documents/Playground/finance/insider-transaction-strategy/backtests.md): reserved for later portfolio/backtest work

If you are not developing the tool, you can ignore most files inside `tool/` and just use the launcher.

## Important Limitation

This project can help answer:

**“Does tracking Form 4 insider-buy clusters seem promising under delayed entry and tradability constraints?”**

It does **not** automatically prove a live trading strategy is robust. The best way to judge that is still:

1. run multiple time windows
2. compare investable vs all events
3. review the case-study verdict
4. verify the result holds up outside one lucky sample
