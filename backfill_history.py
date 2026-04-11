import pandas as pd
import numpy as np
import os
import json

def process_data(file_name):
    print(f"Processing data from {file_name}...")
    dtype_dict_hist = {
        "Close": float,
        "High": float,
        "Low": float,
        "Open": float,
        "Volume": "Int64",
        "Ticker": str
    }
    try:
        df = pd.read_csv(file_name, dtype=dtype_dict_hist, parse_dates=["Date"])
    except FileNotFoundError:
        print(f"File {file_name} not found. Skipping...")
        return pd.DataFrame()
        
    df['Date'] = pd.to_datetime(df['Date'], utc=True, errors='coerce').dt.normalize().dt.tz_localize(None)
    
    unique_tickers = df['Ticker'].unique()
    combined_tick_df = pd.DataFrame()
    for i in unique_tickers:
        tick_df = df[df["Ticker"] == i].sort_values(by="Date").copy()
        numeric_cols = ['Close', 'High', 'Low', 'Open', 'Volume']
        for col_name in numeric_cols:
            tick_df[col_name] = pd.to_numeric(tick_df[col_name], errors='coerce')
        
        tick_df['SMA20'] = tick_df['Close'].rolling(window=20).mean()
        tick_df['SMA50'] = tick_df['Close'].rolling(window=50).mean()
        tick_df['SMA200'] = tick_df['Close'].rolling(window=200).mean()

        ema12 = tick_df['Close'].ewm(span=12, adjust=False).mean()
        ema26 = tick_df['Close'].ewm(span=26, adjust=False).mean()
        tick_df['MACD'] = ema12 - ema26
        tick_df['MACD_Signal'] = tick_df['MACD'].ewm(span=9, adjust=False).mean()

        delta = tick_df['Close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        rs = avg_gain / avg_loss
        tick_df['RSI'] = 100 - (100 / (1 + rs))

        high14 = tick_df['High'].rolling(window=14).max()
        low14 = tick_df['Low'].rolling(window=14).min()
        tick_df['Williams_%R'] = -100 * (high14 - tick_df['Close']) / (high14 - low14)

        tp = (tick_df['High'] + tick_df['Low'] + tick_df['Close']) / 3
        mf = tp * tick_df['Volume']
        pos_mf = mf.where(tp > tp.shift(1), 0)
        neg_mf = mf.where(tp < tp.shift(1), 0)
        rolling_pos = pos_mf.rolling(window=14).sum()
        rolling_neg = neg_mf.rolling(window=14).sum()
        mfr = rolling_pos / (rolling_neg.replace(0, np.nan))
        tick_df['MFI'] = 100 - (100 / (1 + mfr))

        sma_tp = tp.rolling(window=20).mean()
        mad = tp.rolling(window=20).apply(lambda x: (x - x.mean()).abs().mean(), raw=False)
        tick_df['cci'] = (tp - sma_tp) / (0.015 * mad)
        
        combined_tick_df = pd.concat([combined_tick_df, tick_df], ignore_index=True)

    return combined_tick_df

def backfill():
    files = ["all_tickers_3_years_history.csv", "pan1_2year_history.csv", "pan2_2year_history.csv"]
    all_combined_dfs = []
    
    for f in files:
        df = process_data(f)
        if not df.empty:
            all_combined_dfs.append(df)
            
    if not all_combined_dfs:
        print("No data found.")
        return
        
    master_df = pd.concat(all_combined_dfs, ignore_index=True)
    master_df = master_df.drop_duplicates(subset=["Date", "Ticker"]).sort_values(by="Date")
    print("Master DF generated.")
    
    # Calculate categories
    category_list = []
    unique_tickers = master_df['Ticker'].unique()
    for ticker in unique_tickers:
        ticker_df = master_df[master_df['Ticker'] == ticker].copy()
        ticker_df['RSI_prev'] = ticker_df['RSI'].shift(1)
        ticker_df['MACD_prev'] = ticker_df['MACD'].shift(1)
        ticker_df['Williams_%R_prev'] = ticker_df['Williams_%R'].shift(1)
        
        condition_rsi = (ticker_df['RSI'] >= 55) & (ticker_df['RSI'] > ticker_df['RSI_prev'])
        condition_williams = (ticker_df['Williams_%R'] >= -22) & (ticker_df['Williams_%R'] > ticker_df['Williams_%R_prev'])
        condition_macd = ((ticker_df['MACD'] - ticker_df['MACD_Signal']) > 0) & (ticker_df['MACD'] > ticker_df['MACD_prev'])
        
        ticker_df['satisfied_conditions_count'] = condition_rsi.astype(int) + condition_williams.astype(int) + condition_macd.astype(int)
        
        ticker_df['Recommendation'] = 'None'
        ticker_df.loc[ticker_df['satisfied_conditions_count'] == 3, 'Recommendation'] = 'Diamond Pick'
        ticker_df.loc[ticker_df['satisfied_conditions_count'] == 2, 'Recommendation'] = 'Golden Pick'
        ticker_df.loc[ticker_df['satisfied_conditions_count'] == 1, 'Recommendation'] = 'Silver Pick'
        
        category_list.append(ticker_df[['Date', 'Ticker', 'Recommendation']])
        
    category_df = pd.concat(category_list, ignore_index=True)
    
    category_df['Date'] = category_df['Date'].dt.strftime('%Y-%m-%d')
    
    # Let's save as JSON lines or array of objects
    category_records = category_df.to_dict(orient='records')
    with open("category_history.json", "w") as f:
        json.dump(category_records, f, indent=4)
    print("Saved category_history.json")

if __name__ == "__main__":
    backfill()
