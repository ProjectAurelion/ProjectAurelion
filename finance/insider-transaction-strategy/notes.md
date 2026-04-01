# Development of a Systematic Insider-Trading Signal Strategy

### 1. Objective

The purpose of this project is to develop a **systematic methodology for identifying and executing equity trades based on insider purchasing behavior**. The primary objective is to create a **rules-based model** that identifies potential investment opportunities using publicly available insider transaction data, tests the effectiveness of those signals historically, and ultimately generates repeatable buy and sell decisions.

The strategy is motivated by the hypothesis that **corporate insiders possess informational advantages about the future prospects of their companies**, and that significant insider purchases—particularly when multiple insiders buy simultaneously—may signal undervaluation or future positive developments.

This project seeks to evaluate whether those signals can be translated into a **structured and testable investment strategy**.

---

# 2. Hypothesis

The central hypothesis of the model is as follows:

**Cluster purchases by corporate insiders, particularly large purchases made by multiple executives within a short time window, provide a statistically meaningful signal that the underlying company may outperform the broader market over subsequent months.**

More specifically, the hypothesis assumes that the following conditions increase signal strength:

1. Multiple insiders purchasing shares within a short timeframe.
2. Large dollar value purchases relative to typical executive compensation.
3. Purchases occurring after significant declines in stock price.
4. Signals occurring in companies with lower analyst coverage, typically small to mid-cap firms.

If these conditions are consistently associated with improved future performance, they may provide the basis for a **systematic investment strategy**.

---

# 3. Data Sources

To test this hypothesis, two primary categories of data are required.

## 3.1 Insider Transaction Data

Insider transactions in publicly traded companies are disclosed through **SEC regulatory filings**, specifically:

SEC Form 4

These filings report:

* Insider identity
* Corporate role (CEO, CFO, Director, etc.)
* Transaction type (purchase or sale)
* Transaction date
* Filing date
* Shares traded
* Price per share
* Total transaction value

For the purposes of systematic analysis, insider transaction data can be obtained through aggregation platforms such as:

* OpenInsider
* Finviz Insider Trading

These services compile and normalize SEC filing data into structured datasets suitable for quantitative analysis.

---

## 3.2 Market Price Data

To simulate trades and evaluate strategy performance, historical price data is required for all securities under consideration.

Market data can be obtained via APIs such as:

* yfinance

Typical data fields include:

* Date
* Opening price
* High / Low
* Closing price
* Volume

This data enables simulation of entry prices, stop losses, and holding periods.

---

# 4. Analytical Tools

The model will be developed using the Python programming language and several widely used data analysis libraries.

## 4.1 Data Processing

The primary data manipulation library will be:

pandas

Pandas enables:

* Importing insider transaction datasets
* Filtering transactions by criteria
* Grouping transactions by company
* Calculating rolling time windows
* Aggregating insider purchase totals

For example, pandas can identify companies where **multiple insiders purchased shares within a defined time period**.

---

## 4.2 Numerical Analysis

Mathematical operations and return calculations will be performed using:

NumPy

NumPy allows efficient computation of:

* daily returns
* volatility
* portfolio metrics
* statistical indicators

These calculations are necessary for evaluating the profitability and risk profile of the strategy.

---

## 4.3 Backtesting Framework

To simulate trades and evaluate portfolio performance, the project will utilize a backtesting engine such as:

Backtrader

Backtesting frameworks allow researchers to simulate:

* trade execution
* portfolio allocation
* stop-loss rules
* holding periods
* profit targets
* portfolio value over time

The framework ensures that the strategy is evaluated under **realistic historical conditions**.

---

# 5. Strategy Construction

The strategy consists of several components: signal identification, trade entry, exit rules, and portfolio management.

---

## 5.1 Signal Identification

The model identifies potential trade opportunities based on insider purchase clusters.

Example filtering criteria may include:

* Insider purchases exceeding $100,000 in total value
* At least two insiders purchasing shares within 30 days
* Insider roles including officers or directors
* Companies with market capitalizations below $5 billion
* Stocks trading above $5 per share
* Minimum liquidity thresholds (e.g., average daily volume above 300,000 shares)

These filters aim to eliminate noise from small or symbolic insider transactions.

---

## 5.2 Entry Rules

Once a signal is identified, the strategy must determine when a trade is executed.

The model will assume:

**Entry occurs at the next available market price following the public filing of the insider transaction.**

This constraint ensures that the backtest only uses information that was publicly available at the time.

---

## 5.3 Exit Rules

Positions will be exited based on predetermined conditions.

Typical rules may include:

* Stop loss (e.g., −20%)
* Profit target (e.g., +50%)
* Maximum holding period (e.g., 12 months)

These parameters limit downside risk while allowing profitable trades to develop.

---

# 6. Backtesting Methodology

The model evaluates strategy performance through historical simulation.

The backtesting process proceeds as follows:

1. Load historical insider transaction data.
2. Identify qualifying purchase clusters based on predefined filters.
3. Retrieve historical stock prices for each qualifying company.
4. Simulate trade entries following the insider filing date.
5. Track each position over time.
6. Exit positions when stop loss, profit target, or time limit conditions occur.
7. Record trade outcomes.

Each simulated trade is stored in a transaction log containing:

* ticker symbol
* entry date
* entry price
* exit date
* exit price
* percentage return

---

# 7. Performance Evaluation

The model evaluates strategy performance using several financial metrics.

These include:

### Return Metrics

* Annualized return
* Average trade return
* Median trade return

### Risk Metrics

* Maximum drawdown
* Volatility
* Sharpe ratio

### Trade Statistics

* Win rate
* Average gain vs. average loss
* Profit factor

These metrics allow comparison of the insider strategy against benchmark portfolios such as market index funds.

---

# 8. Bias and Model Validation

Backtesting requires careful controls to avoid statistical errors.

Important considerations include:

### Look-Ahead Bias

Trades must be triggered using the **filing date**, not the transaction date.

### Survivorship Bias

The dataset must include companies that no longer exist to prevent artificially optimistic results.

### Liquidity Constraints

Trades should only occur in securities with sufficient trading volume to realistically execute orders.

### Overfitting

Strategy parameters should be tested across multiple time periods to ensure robustness.

---

# 9. Model Implementation and Practical Use

Once validated, the model can be used operationally to generate trade signals.

The workflow would consist of:

1. Daily download of newly filed insider transactions.
2. Automatic filtering based on strategy criteria.
3. Generation of a list of potential trade candidates.
4. Manual or automated execution of trades.
5. Continuous monitoring of positions according to exit rules.

This approach transforms insider transaction data into a **systematic trading process rather than discretionary speculation**.

---

# 10. Conclusion

This project outlines a structured approach to testing whether **cluster insider purchases can serve as a predictive investment signal**.

By combining insider transaction data, historical price data, and quantitative backtesting tools, it is possible to construct a model that:

* identifies potential opportunities
* evaluates historical performance
* generates objective buy and sell signals

While insider-based strategies may provide statistical advantages in certain conditions, their effectiveness must be validated through rigorous testing and careful risk management.

Ultimately, the objective is not to predict individual stock movements with certainty but rather to determine whether insider purchasing patterns provide **a repeatable probabilistic edge within a diversified investment framework**.

