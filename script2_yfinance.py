import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

# 📅 Calcola le date (oggi e ieri)
today = datetime.now()
yesterday = today - timedelta(days=1)
start_date = yesterday.strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")
date_str = yesterday.strftime("%Y-%m-%d")  # Data usata per nome file

# 📥 Carica ticker dal CSV creato dallo script 1
ticker_file = f"output/tickers_{end_date}.csv"
df_tickers = pd.read_csv(ticker_file)
tickers = df_tickers['Ticker'].tolist()

intervals = {
    "1m": "1m",
    "5m": "5m",
    "30m": "30m",
    "1h": "60m",
    "90m": "90m",
    "4h": "4h"
}

final_rows = []

for ticker in tickers:
    print(f"📅 Ticker: {ticker}")
    stock = yf.Ticker(ticker)

    try:
        info = stock.info
        float_shares = info.get("floatShares")
        if float_shares is not None and float_shares > 50000000:
            print(f"❌ Float troppo alto ({float_shares}), skippo...")
            continue

        fundamentals = {
            "Ticker": ticker,
            "Market Cap": info.get("marketCap"),
            "Float Shares": float_shares,
            "Insider Ownership": info.get("heldPercentInsiders"),
            "Institutional Ownership": info.get("heldPercentInstitutions")
        }
    except Exception as e:
        print(f"⚠️ Errore info {ticker}: {e}")
        continue

    daily = stock.history(start=start_date, end=end_date, interval="1d").reset_index()
    for _, row in daily.iterrows():
        day = row["Date"].date()
        prev_day = stock.history(
            start=(day - timedelta(days=5)).strftime('%Y-%m-%d'),
            end=day.strftime('%Y-%m-%d'),
            interval="1d"
        )
        prev_close = prev_day["Close"].iloc[-1] if not prev_day.empty else None

        open_price = row["Open"]
        data = fundamentals.copy()
        data["Date"] = day
        data["Open"] = open_price
        data["Prev Close"] = prev_close
        data["High"] = row["High"]
        data["Low"] = row["Low"]
        data["Close"] = row["Close"]
        data["Volume"] = row["Volume"]

        if prev_close and open_price:
            gap_pct = round(((open_price - prev_close) / prev_close) * 100)
        else:
            gap_pct = None
        data["Gap %"] = gap_pct

        if gap_pct is None or gap_pct < 25:
            print(f"❌ Gap < 25%, skippo...")
            continue

        vwap_value = None
        hist_1m = stock.history(
            start=day.strftime('%Y-%m-%d'),
            end=(day + timedelta(days=1)).strftime('%Y-%m-%d'),
            interval="1m"
        )
        if not hist_1m.empty:
            hist_1m = hist_1m.tz_localize(None)
            vwap_value = (hist_1m["Close"] * hist_1m["Volume"]).sum() / hist_1m["Volume"].sum()
            vwap_pct = round(((vwap_value - open_price) / open_price) * 100)
