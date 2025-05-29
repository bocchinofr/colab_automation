import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

# 📅 Data
date_str = datetime.now().strftime("%Y-%m-%d")

# 📥 Carica ticker dal CSV creato dallo script 1
ticker_file = f"output/tickers_{date_str}.csv"
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
            data["VWAP"] = round(vwap_value, 2)
            data["VWAP %"] = vwap_pct

            premarket = hist_1m[hist_1m.index.time < datetime.strptime("09:30", "%H:%M").time()]
            if not premarket.empty:
                high_pm = premarket["High"].max()
                data["High Pre-Market"] = round(high_pm, 2)
                data["Open vs Pre-Market %"] = round(((open_price - high_pm) / high_pm) * 100)
            else:
                data["High Pre-Market"] = None
                data["Open vs Pre-Market %"] = None

        else:
            data["VWAP"] = None
            data["VWAP %"] = None
            data["High Pre-Market"] = None
            data["Open vs Pre-Market %"] = None

        broke_pmh = {}
        for label, interval in intervals.items():
            try:
                intraday = stock.history(
                    start=day.strftime('%Y-%m-%d'),
                    end=(day + timedelta(days=1)).strftime('%Y-%m-%d'),
                    interval=interval
                )
                if not intraday.empty:
                    intraday = intraday.tz_localize(None)
                    high = intraday["High"].max()
                    low = intraday["Low"].min()
                    vol = intraday["Volume"].sum()
                    data[f"High_{label}"] = round((high / open_price - 1) * 100)
                    data[f"Low_{label}"] = round((low / open_price - 1) * 100)
                    data[f"Volume_{label}"] = vol

                    if data["High Pre-Market"] is not None:
                        broke = "si" if high > data["High Pre-Market"] else "no"
                    else:
                        broke = "n/a"
                    data[f"Break_PMH_{label}"] = broke
                else:
                    data[f"High_{label}"] = None
                    data[f"Low_{label}"] = None
                    data[f"Volume_{label}"] = None
                    data[f"Break_PMH_{label}"] = "n/a"
            except Exception as e:
                print(f"⚠️ Errore {ticker} - {interval}: {e}")
                data[f"High_{label}"] = None
                data[f"Low_{label}"] = None
                data[f"Volume_{label}"] = None
                data[f"Break_PMH_{label}"] = "n/a"

        if data.get("Volume_1m", 0) < 700_000:
            print(f"❌ Volume 1m < 700k, skippo...")
            continue

        final_rows.append(data)

# 📤 Output finale
output_file = f"output/dati_azioni_completo_{date_str}.xlsx"

# Il resto del codice resta invariato, cambia solo la riga di export finale:
df_final = pd.DataFrame(final_rows)
df_final.to_excel(output_file, index=False)
print(f"✅ File generato: {output_file}")
