import os

def rewrite_wrappers():
    files_info = [
        ("pan_portfolio_dropbox.py", "5_days_history_pan.csv", "all_tickers_3_years_history.csv"),
        ("pan_1_portfolio_dropbox.py", "5_days_history_pan1.csv", "pan1_2year_history.csv"),
        ("pan_2_portfolio_dropbox.py", "5_days_history_pan2.csv", "pan2_2year_history.csv"),
    ]

    for fname, f1, f2 in files_info:
        with open(fname, "r", encoding='utf-8') as f:
            lines = f.readlines()
            
        tickers_str = ""
        in_tickers = False
        for line in lines:
            if "tickers = sorted(list(set([" in line:
                in_tickers = True
            
            if in_tickers:
                tickers_str += line
                if "]" in line and ")))" in line:
                    in_tickers = False
                    break
        
        wrapper_content = f"""# -*- coding: utf-8 -*-
from portfolio_utils import run_portfolio

{tickers_str}
if __name__ == "__main__":
    run_portfolio(tickers, "{f1}", "{f2}")
"""
        with open(fname, "w", encoding='utf-8') as f:
            f.write(wrapper_content)

if __name__ == "__main__":
    rewrite_wrappers()
