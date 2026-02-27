import yfinance as yf
import pandas as pd
import os

# Define tickers
tickers = sorted(list(set([
    "AAVAS.NS","ADANIPORTS.NS","AFFLE.NS","APLAPOLLO.NS","APARINDS.NS","APOLLOTYRE.NS",
    "ARE&M.BO","BAJAJHFL.NS","BANDHANBNK.BO","BBTC.NS","BECTORFOOD.NS","CIPLA.NS",
    "COFORGE.NS","DPWIRES.NS","DRREDDY.NS","ELECON.NS","ENDURANCE.NS","ENGINERSIN.NS",
    "ETERNAL.NS","FCL.BO","FINCABLES.BO","FINPIPE.NS","GENUSPOWER.NS","GOLDBEES.NS",
    "GOLDIAM.NS","HAPPSTMNDS.NS","HDFCBANK.BO","HINDUNILVR.BO","HINDWAREAP.BO",
    "HINDZINC.NS","HSCL.NS","IDFCFIRSTB.NS","IGIL.BO","INDGELA.BO","INDHOTEL.NS",
    "INFY.NS","ITC.NS","ITI.NS","JINDALSAW.NS","JKIL.BO","JSL.NS","JSWINFRA.NS",
    "KIRLOSBROS.NS","KIRLOSENG.NS","KPITTECH.NS","LTTS.NS","LUPIN.BO","MANKIND.NS",
    "MARKSANS.NS","NEWGEN.NS","ONGC.NS","PRAKASH.BO","PRAJIND.NS","PRICOLLTD.BO",
    "POLICYBZR.NS","RCF.NS","REFEX.NS","RICOAUTO.NS","ROSSARI.BO","RPPINFRA.NS",
    "RVNL.NS","SANGHVIMOV.BO","SBIN.NS","SETFNIF50.BO","TANLA.NS","TCS.NS",
    "TTKHLTCARE.NS","VENUSPIPES.NS","WABAG.NS","WAAREERTL.BO","ZEEL.NS","360ONE.NS"
])))

output_file = "all_tickers_3_years_history.csv"

# Remove existing file if you want a fresh 3-year reload every time the action runs
if os.path.exists(output_file):
    os.remove(output_file)

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
        
        # Filter and Rename logic from your code
        # Note: We use 'Price' because that's what your original filtering logic looked for
        data_df_filtered = data_df[~data_df['Price'].isin(['Ticker', 'Date'])]
        data_df_filtered = data_df_filtered.rename(columns={'Price': 'Date'})
        
        # Append to main file
        write_header = not os.path.exists(output_file)
        data_df_filtered.to_csv(output_file, mode='a', index=False, header=write_header)
        
        os.remove(temp_csv) 
        print(f"Data for {i} processed.")
    except Exception as e:
        print(f"Error processing {i}: {e}")
