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

# Timeframes da aggregare da dati 1m
resample_map = {
    "1m": "1min",
    "5m": "5min",
    "30m": "30min",
    "1h": "1h",
    "90m": "90min",
    "4h": "4h"
}

final_rows = []

for ticker in tickers:
    print(f"\n📈 Analizzo: {ticker}")
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

        # Dati 1m per calcolo Open reale
        hist_1m = stock.history(
            start=day.strftime('%Y-%m-%d'),
            end=(day + timedelta(days=1)).strftime('%Y-%m-%d'),
            interval="1m"
        )

        if hist_1m.empty:
            print(f"⚠️ Nessun dato 1m per {ticker}, skippo...")
            continue

        # ✅ Correggi la gestione del fuso orario
        if hist_1m.index.tz is None:
            hist_1m.index = hist_1m.index.tz_localize("UTC").tz_convert("America/New_York")
        else:
            hist_1m.index = hist_1m.index.tz_convert("America/New_York")
        hist_1m = hist_1m.sort_index()

        print(hist_1m.between_time("09:30", "16:00").head())
        print(hist_1m.between_time("09:30", "16:00").tail())

        market_open_time = pd.Timestamp(datetime.combine(day, datetime.strptime("09:30", "%H:%M").time()), tz="America/New_York")

        try:
            open_price = hist_1m.loc[market_open_time]["Open"]
        except:
            print(f"⚠️ Nessun dato alle 09:30 per {ticker}, skippo...")
            continue

        data = fundamentals.copy()
        data.update({
            "Date": day,
            "Open": open_price,
            "Prev Close": prev_close,
            "High": row["High"],
            "Low": row["Low"],
            "Close": row["Close"],
            "Volume": row["Volume"],
            "Open (Daily)": row["Open"]
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

        # Aggregazione intraday da 1m
        intraday_market = hist_1m.between_time("09:30", "16:00")
        print(f"🎯 Intraday rows (09:30–16:00): {len(intraday_market)}")

        if len(intraday_market) < 30:
            print(f"❌ Troppi pochi dati intraday per {ticker}, skippo aggregazione...")
            continue

        for label, resample_rule in resample_map.items():
            try:
                agg = (
                    intraday_market
                    .resample(resample_rule, origin='start', offset="0min")
                    .agg({
                        "High": "max",
                        "Low": "min",
                        "Volume": "sum"
                    })
                    .dropna()
                )
                if label == "1m":
                    print(f"🔍 Primo minuto aggregato 1m per {ticker}:")
                    print(agg.head(3))


                high = agg["High"].max()
                low = agg["Low"].min()
                vol = agg["Volume"].sum()

                data[f"High_{label}"] = round(high, 2)
                data[f"Low_{label}"] = round(low, 2)
                data[f"Volume_{label}"] = int(vol)
                
                print(f"📊 {ticker} - {label} | High: {high:.2f}, Low: {low:.2f}, Volume: {vol:,}")

                if data["High Pre-Market"] is not None:
                    data[f"Break_PMH_{label}"] = "si" if high > data["High Pre-Market"] else "no"
                else:
                    data[f"Break_PMH_{label}"] = "n/a"

            except Exception as e:
                print(f"⚠️ Errore aggregazione {ticker} - {label}: {e}")
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
