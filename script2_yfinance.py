import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# 📅 Calcola le date
today = datetime.now()
yesterday = today - timedelta(days=1)
start_date = yesterday.strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")
date_str = yesterday.strftime("%Y-%m-%d")

# 📥 Legge i ticker generati dal primo script
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
    print(f"📈 Analizzo: {ticker}")
    stock = yf.Ticker(ticker)

    # Dati fondamentali
    try:
        info = stock.info
        float_shares = info.get("floatShares")
        if float_shares is not None and float_shares > 50_000_000:
            print(f"❌ Float troppo alto ({float_shares}), skippo...")
            continue

        fundamentals = {
            "Ticker": ticker,
            "Market Cap": info.get("marketCap"),
            "Shares Outstanding": info.get("sharesOutstanding"),
            "Float Shares": float_shares,
            "Insider Ownership": info.get("heldPercentInsiders"),
            "Institutional Ownership": info.get("heldPercentInstitutions")
        }

    except Exception as e:
        print(f"⚠️ Errore nei fondamentali: {e}")
        continue

    # Dati daily
    daily = stock.history(start=start_date, end=end_date, interval="1d").reset_index()
    for _, row in daily.iterrows():
        day = row["Date"].date()

        # Chiusura giorno precedente
        prev_day = stock.history(
            start=(day - timedelta(days=5)).strftime('%Y-%m-%d'),
            end=day.strftime('%Y-%m-%d'),
            interval="1d"
        )
        prev_close = prev_day["Close"].iloc[-1] if not prev_day.empty else None
        open_price = row["Open"]

        data = fundamentals.copy()
        data.update({
            "Date": day,
            "Open": open_price,
            "Prev Close": prev_close,
            "High": row["High"],
            "Low": row["Low"],
            "Close": row["Close"],
            "Volume": row["Volume"]
        })

        # Gap%
        if prev_close and open_price:
            gap_pct = round(((open_price - prev_close) / prev_close) * 100)
        else:
            gap_pct = None
        data["Gap %"] = gap_pct

        if gap_pct is None or gap_pct < 25:
            print("❌ Gap < 25%, skippo...")
            continue

        # Intraday 1m
        hist_1m = stock.history(
            start=day.strftime('%Y-%m-%d'),
            end=(day + timedelta(days=1)).strftime('%Y-%m-%d'),
            interval="1m"
        )

        if not hist_1m.empty:
            hist_1m = hist_1m.tz_localize(None)

            # VWAP
            vwap = (hist_1m["Close"] * hist_1m["Volume"]).sum() / hist_1m["Volume"].sum()
            data["VWAP"] = round(vwap, 2)
            data["VWAP %"] = round(((vwap - open_price) / open_price) * 100, 2)

            # High pre-market
            premarket = hist_1m[hist_1m.index.time < datetime.strptime("09:30", "%H:%M").time()]
            if not premarket.empty:
                high_pm = premarket["High"].max()
                data["High Pre-Market"] = round(high_pm, 2)
                data["Open vs Pre-Market %"] = round(((open_price - high_pm) / high_pm) * 100, 2)
            else:
                data["High Pre-Market"] = None
                data["Open vs Pre-Market %"] = None
        else:
            data["VWAP"] = None
            data["VWAP %"] = None
            data["High Pre-Market"] = None
            data["Open vs Pre-Market %"] = None

        # Intervalli intraday
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

                    data[f"High_{label}"] = round((high / open_price - 1) * 100, 2)
                    data[f"Low_{label}"] = round((low / open_price - 1) * 100, 2)
                    data[f"Volume_{label}"] = int(vol)

                    if data["High Pre-Market"] is not None:
                        data[f"Break_PMH_{label}"] = "si" if high > data["High Pre-Market"] else "no"
                    else:
                        data[f"Break_PMH_{label}"] = "n/a"
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

        # Filtro finale: Volume 1m
        if data.get("Volume_1m", 0) < 700_000:
            print("❌ Volume 1m < 700k, skippo...")
            continue

        final_rows.append(data)

# 📤 Salva file Excel
df_final = pd.DataFrame(final_rows)
output_path = f"output/dati_azioni_completo_{date_str}.xlsx"
df_final.to_excel(output_path, index=False)
print(f"✅ File salvato: {output_path}")
