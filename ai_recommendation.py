import json
import pandas as pd
import os
from google import genai
from google.genai import types
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

def run_ai_recommendations():
    print("Starting AI Recommendation Script...")
    
    # 1. Load the category history
    try:
        with open('category_history.json', 'r') as f:
            cat_hist = json.load(f)
        df_cat = pd.DataFrame(cat_hist)
        df_cat['Date'] = pd.to_datetime(df_cat['Date'])
        df_cat = df_cat.sort_values(by=['Ticker', 'Date'])
    except Exception as e:
        print(f"Error loading category history: {e}")
        return

    print("Calculating transition probabilities...")
    transition_counts = {}
    for ticker in df_cat['Ticker'].unique():
        t_df = df_cat[df_cat['Ticker'] == ticker].copy()
        t_df['Prev_Rec'] = t_df['Recommendation'].shift(1)
        
        jumps = {
            'Silver -> Gold': 0,
            'Gold -> Diamond': 0,
            'Silver -> Diamond': 0,
            'None -> Silver': 0,
            'None -> Gold': 0,
            'None -> Diamond': 0
        }
        
        for _, row in t_df.iterrows():
            prev = row['Prev_Rec']
            curr = row['Recommendation']
            if pd.isna(prev): continue
            if prev == 'Silver Pick' and curr == 'Golden Pick': jumps['Silver -> Gold'] += 1
            elif prev == 'Golden Pick' and curr == 'Diamond Pick': jumps['Gold -> Diamond'] += 1
            elif prev == 'Silver Pick' and curr == 'Diamond Pick': jumps['Silver -> Diamond'] += 1
            elif prev == 'None' and curr == 'Silver Pick': jumps['None -> Silver'] += 1
            elif prev == 'None' and curr == 'Golden Pick': jumps['None -> Gold'] += 1
            elif prev == 'None' and curr == 'Diamond Pick': jumps['None -> Diamond'] += 1
            
        transition_counts[ticker] = jumps

    # 2. Get the latest ticking metrics
    latest_date = df_cat['Date'].max()
    latest_df = df_cat[df_cat['Date'] == latest_date]
    shortlisted = latest_df[latest_df['Recommendation'] != 'None']['Ticker'].tolist()

    if not shortlisted:
        print("No tickers shortlisted on the latest date.")
        return

    # 3. Get indicators for the shortlisted
    print("Loading indicators...")
    try:
        with open('indicators_history.json', 'r') as f:
            ind_hist = json.load(f)
        ind_df = pd.DataFrame(ind_hist)
        latest_ind = ind_df[ind_df['Ticker'].isin(shortlisted)].sort_values('Date', ascending=False).groupby('Ticker').head(1)
    except Exception as e:
        print(f"No indicators history retrieved: {e}")
        latest_ind = pd.DataFrame()

    # 4. Get Backtesting Data
    print("Loading backtesting data...")
    try:
        with open('backtesting_results.json', 'r') as f:
            bt_hist = json.load(f)
        bt_df = pd.DataFrame(bt_hist)
        bt_df = bt_df.sort_values('run_date', ascending=False)
    except Exception as e:
        print(f"No backtesting history retrieved: {e}")
        bt_df = pd.DataFrame()

    # 5. Format the context for Gemini
    prompt_data = []
    for ticker in shortlisted:
        rec = latest_df[latest_df['Ticker'] == ticker]['Recommendation'].iloc[0]
        jumps = transition_counts.get(ticker, {})
        
        # Format indicators
        indi_strs = "N/A"
        if not latest_ind.empty:
            t_ind = latest_ind[latest_ind['Ticker'] == ticker]
            if not t_ind.empty:
                r = t_ind.iloc[0]
                def fmt(val):
                    try:
                        return f"{float(val):.2f}"
                    except (ValueError, TypeError):
                        return "N/A"
                indi_strs = f"Price: {fmt(r.get('Close'))}, RSI: {fmt(r.get('RSI'))}, MACD: {fmt(r.get('MACD'))}, WM%R: {fmt(r.get('Williams_%R'))}, SMA20: {fmt(r.get('SMA20'))}, SMA50: {fmt(r.get('SMA50'))}, SMA200: {fmt(r.get('SMA200'))}, A/D Line: {fmt(r.get('AD_Line'))}"
                
        # Format backtest
        bt_strs = "N/A"
        if not bt_df.empty:
            t_bt = bt_df[(bt_df['Ticker'] == ticker) & (bt_df['Metric_Type'] == 'Original Picks')]
            if not t_bt.empty:
                r2 = t_bt.iloc[0]
                bt_strs = f"Win Rate: {r2.get('Win Rate (%)', 'N/A')}%, Avg Win Return: {r2.get('Average Winning Return (%)', 'N/A')}%"
                
        prompt_data.append(f"- Ticker: {ticker} | Category: {rec} | Jumps History: {jumps} | Latest Techs: {indi_strs} | Backtest: {bt_strs}")

    prompt_context = "\n".join(prompt_data)
    print(f"Found {len(shortlisted)} tickers to analyze. Calling Gemini...")
    
    api_key = os.environ.get("ticker_fomo_news_gemini_api")
    if not api_key:
        api_key = os.environ.get("GEMINI_API_KEY")
        
    if not api_key:
        print("Error: Gemini API Key not found.")
        return

    # Call Gemini API
    client = genai.Client(api_key=api_key)

    prompt = f"""You are an elite quantitative AI stock analyst focusing on the Indian equity market (NSE). 
I have a list of shortlisted stocks with their current momentum category (Diamond/Gold/Silver), their historical propensity to upgrade categories (e.g. jumping from Silver to Diamond), their latest technical indicators, and their algorithmic backtesting win-rate.

<SHORTLISTED_STOCKS_DATA>
{prompt_context}
</SHORTLISTED_STOCKS_DATA>

Your Instructions:
1. Scrutinize the algorithmic data above. Prioritize stocks with strong Backtesting Win Rates (>60% if possible) and a history of upward category jumps.
2. Evaluate the Expanded Technicals: Ensure the current `Price` is logically supported by its moving averages (`SMA20`, `SMA50`, `SMA200`). Strong recommendations should ideally be above their SMA200 (long term safety) and show volume accumulation validation via the `A/D Line`.
3. Use Google Search to fetch the extremely LATEST FOMO news, earnings catalysts, or sector rotations affecting these specific tickers in the Indian Stock Market. Note: Add '.NS' or 'NSE' for relevance.
4. Merge the quantitative signals with the real-world news catalysts to select the ABSOLUTE BEST "sure shot" stock recommendations for the very next trading session. Ignore the mediocre ones.
5. Output a clean HTML email body consisting of:
    - An introductory sentence.
    - A list or table of the highly confident "Sure Shot" picks.
    - For each pick, explain WHY it was selected (cite the algorithm stats + the fresh news sentiment).
    - Do NOT output ```html markdown code blocks. Just output raw HTML tags that can be directly embedded into an email.
"""

    config = types.GenerateContentConfig(
        tools=[{"google_search": {}}]
    )
    
    try:
        response = client.models.generate_content(
            model='gemini-3.1-flash-lite',
            contents=prompt,
            config=config
        )
        html_body = response.text
    except Exception as e:
        print(f"Error calling Gemini: {e}")
        # fallback to flash macro if 3.1-flash-lite is not available to the key
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=config
        )
        html_body = response.text

    if html_body.startswith('```html'):
        html_body = html_body[7:]
    if html_body.endswith('```'):
        html_body = html_body[:-3]

    print("Gemini Analysis complete. Dispatching email...")
    
    # 6. Dispatch Email
    gmail_address = os.environ.get('GMAIL_ADDRESS')
    gmail_app_password = os.environ.get('GMAIL_APP_PASSWORD')
    if not gmail_address or not gmail_app_password:
        print("Missing email credentials.")
        return

    email_list = [e.strip() for e in gmail_address.split(",")]
    sender_email = email_list[0]
    receiver_emails = email_list

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_emails)
    msg['Subject'] = "🌟 AI Sure-Shot FOMO Stock Recommendations"
    msg.attach(MIMEText(html_body.strip(), 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, gmail_app_password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())
        print("Recommendations email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    run_ai_recommendations()
