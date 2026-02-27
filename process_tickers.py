import yfinance as yf
import pandas as pd
import os

# Define tickers
tickers = sorted(list(set([
    "AAVAS.NS","ADANIPORTS.NS","AFFLE.NS","APLAPOLLO.NS","APARINDS.NS","APOLLOTYRE.NS"
    # ... add the rest of your tickers here
])))

output_file = "all_tickers_3_years_history.csv"

for i in tickers:
    try:
        # Download data
        data = yf.download(i, period="3y", interval="1d", auto_adjust=True)
        if data.empty:
            continue
            
        temp_csv = f"{i}.csv"
        data.to_csv(temp_csv)
        
        # Reload to process (matching your original logic)
        data_df = pd.read_csv(temp_csv)
        data_df['Ticker'] = i
        
        # Filter and Rename
        data_df_filtered = data_df[~data_df['Price'].isin(['Ticker', 'Date'])]
        data_df_filtered = data_df_filtered.rename(columns={'Price': 'Date'})
        
        # Append to main file
        write_header = not os.path.exists(output_file)
        data_df_filtered.to_csv(output_file, mode='a', index=False, header=write_header)
        
        os.remove(temp_csv) # Clean up individual files
        print(f"Data for {i} processed.")
    except Exception as e:
        print(f"Error processing {i}: {e}")
