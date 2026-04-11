import os

def create_utils():
    with open("pan_portfolio_dropbox.py", "r", encoding='utf-8') as f:
        lines = f.readlines()

    out_lines = [
        "import yfinance as yf\n",
        "import pandas as pd\n",
        "import numpy as np\n",
        "import os\n",
        "from datetime import datetime\n",
        "import dropbox\n",
        "import smtplib\n",
        "from email.mime.text import MIMEText\n",
        "from email.mime.multipart import MIMEMultipart\n",
        "import matplotlib.pyplot as plt\n",
        "import matplotlib.dates as mdates\n\n",
        "def run_portfolio(tickers, five_days_history_filename, target_history_filename):\n"
    ]

    skip = False
    in_imports = False
    for line in lines:
        if line.startswith("import ") or line.startswith("from "):
            continue
        if line.startswith("# -*- coding: utf-8 -*-"):
            continue
        if line.startswith("pd.options.mode.chained_assignment = None"):
            out_lines.insert(2, "pd.options.mode.chained_assignment = None\n")
            continue
            
        if "tickers = sorted(list(set" in line:
            skip = True
        if skip:
            if "]" in line and ")))" in line:
                skip = False
            continue

        # Replace strings with variables
        l = line
        l = l.replace('"all_tickers_3_years_history.csv"', 'target_history_filename')
        l = l.replace("'5_days_history_pan.csv'", 'five_days_history_filename')
        l = l.replace('"5_days_history_pan.csv"', 'five_days_history_filename')
        
        # Enforce column selection before save if not present
        if 'merged_df_pandas.to_csv(target_history_filename' in l:
           out_lines.append("    cols_to_read = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Volume']\n")
           out_lines.append("    merged_df_pandas = merged_df_pandas[cols_to_read]\n")
           out_lines.append("    " + l)
           continue
           
        if 'pd.read_csv(target_history_filename' in l and 'usecols' not in l:
            # handle pan_1 and pan which don't have usecols
            l = l.replace('dtype= dtype_dict_hist', 'usecols=[\'Date\', \'Ticker\', \'Close\', \'High\', \'Low\', \'Open\', \'Volume\'], dtype=dtype_dict_hist')
            
        out_lines.append("    " + l)

    with open("portfolio_utils.py", "w", encoding='utf-8') as f:
        f.writelines(out_lines)

if __name__ == "__main__":
    create_utils()
