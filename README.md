# Portfolio Stock Indicator Analysis

This application automates the process of fetching daily historical stock data for various baskets of tickers, running technical analysis to compute index values (SMA, MACD, RSI, Williams %R, MFI, CCI, OBV, A/D Line), and filtering stocks into actionable trading recommendations. It ultimately sends an email with the shortlisted tickers, historical backtesting metrics, and Dropbox links to generated comparison charts.

## Trading Categories and Indicators

The application shortlists stocks based on short-term momentum oscillators. The original stock filtering criteria evaluates how many of three primary technical conditions are met:

- **Condition 1 (RSI)**: `RSI >= 55` AND it is increasing (`RSI > RSI_prev`).
- **Condition 2 (Williams %R)**: `Williams %R >= -22` AND it is increasing (`Williams %R > Williams %R_prev`).
- **Condition 3 (MACD)**: MACD is above its Signal Line (`MACD - MACD_Signal > 0`) AND it is increasing (`MACD > MACD_prev`).

### Pick Categories
Based on the conditions defined above, the shortlisted stocks are assigned into following performance tiers:
- **Diamond Pick**: All 3 conditions are satisfied.
- **Golden Pick**: Any 2 of the 3 conditions are satisfied.
- **Silver Pick**: Any 1 of the 3 conditions is satisfied.

*(Note: The system additionally evaluates whether the Commodity Channel Index (`CCI`) is strictly between `-200` and `200` although this doesn't directly influence the core condition score.)*

---

## Advanced Technical Trading Signals

Beyond the basic shortlisting, the app tracks and backtests historical performance using three distinct, stricter compound trading signals. A signal is only triggered when specific technical indicators align.

For all three signals, there are strict foundation requirements:
- **MACD Support**: The `MACD` must be strictly bullish (i.e. `MACD > MACD_Signal` AND `MACD > 0`).
- **Volume/Momentum Confirmation**: At least **one** of the following must be true:
    - `-200 < CCI < 200`
    - `OBV > OBV_prev` (On-Balance Volume is increasing)
    - `Volume > 1.5 * Volume_20_day_SMA` (Current daily volume is at least 150% of the 20-day Simple Moving Average volume)

### Signal 1
Focuses on early momentum shifts indicating potential breakouts.
- **RSI Range**: `55 <= RSI <= 60`
- **Williams %R Range**: `-22 <= Williams %R <= -16`
- Includes the foundational MACD and Volume/Momentum requirements.

### Signal 2
Focuses on sustained momentum with slightly more definitive upswings.
- **RSI Range**: `60 < RSI <= 65`
- **Williams %R Range**: `-20 < Williams %R <= -9`
- Includes the foundational MACD and Volume/Momentum requirements.

### Signal 3
Focuses on mature momentum prior to hitting severe overbought limits.
- **RSI Range**: `65 < RSI <= 70`
- **Williams %R Range**: `-16 < Williams %R <= -2`
- Includes the foundational MACD and Volume/Momentum requirements.

---

## Code Configuration and Execution
This repository utilizes a shared architecture where the core execution logic, data scraping, indicator plotting, backtesting, and cloud integration (Dropbox uploads + Email alerts) are securely structured within `portfolio_utils.py`. The individual executed Python files (`pan_portfolio_dropbox.py`, `pan_1_portfolio_dropbox.py`, etc.) act as simple orchestration wrappers which feed unique portfolios of tickers to the primary function. All of this is integrated natively within a GitHub Actions cron schedule.
