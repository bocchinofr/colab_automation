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
df_tickers = pd.read_csv(ticker_file, keep_default_na=False)
tickers = df_tickers['Ticker'].dropna().unique().tolist()

# Mappa dei ticker -> valori Finviz
finviz_map = df_tickers.set_index('Ticker').to_dict('index')

# Timeframes per resample "classico"
resample_map = {
    "1m": "1min",
    "5m": "5min"
}

final_rows = []

for ticker in tickers:
    print(f"\n📈 Analizzo: {ticker}")
    stock = yf.Ticker(ticker)

    # Dati fondamentali
    try:
        info = stock.info
        yf_float = info.get("floatShares")
        yf_outstanding = info.get("sharesOutstanding")

        # Recupero dati Finviz (se presenti)
        finviz_float = finviz_map.get(ticker, {}).get("Shs Float")
        finviz_out = finviz_map.get(ticker, {}).get("Shs Outstanding")

        # Converte valori Finviz in numerici (gestendo eventuali formati $m / $b / K)
        def parse_finviz_shares(x):
            if x is None or x == '':
                return None
            x = str(x).replace(',', '').replace('$', '').strip()
            if x.endswith('M'):
                return float(x[:-1]) * 1e6
            elif x.endswith('B'):
                return float(x[:-1]) * 1e9
            elif x.endswith('K'):
                return float(x[:-1]) * 1e3
            else:
                try:
                    return float(x)
                except:
                    return None

        finviz_float = parse_finviz_shares(finviz_float)
        finviz_out = parse_finviz_shares(finviz_out)

        # Scegli il valore minimo disponibile (Yahoo o Finviz)
        float_shares = min([v for v in [yf_float, finviz_float] if v is not None], default=None)
        shares_out = min([v for v in [yf_outstanding, finviz_out] if v is not None], default=None)

        fundamentals = {
            "Ticker": ticker,
            "Market Cap": info.get("marketCap"),
            "Shares Outstanding": shares_out,
            "Float Shares": float_shares,
            "Insider Ownership": info.get("heldPercentInsiders"),
            "Institutional Ownership": info.get("heldPercentInstitutions")
        }

        if float_shares is not None and float_shares > 50_000_000:
            print(f"❌ Float troppo alto ({float_shares}), skippo...")
            continue

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

        # ✅ Gestione fuso orario
        if hist_1m.index.tz is None:
            hist_1m.index = hist_1m.index.tz_localize("UTC").tz_convert("America/New_York")
        else:
            hist_1m.index = hist_1m.index.tz_convert("America/New_York")
        hist_1m = hist_1m.sort_index()

        market_open_time = pd.Timestamp(datetime.combine(day, datetime.strptime("09:30", "%H:%M").time()), tz="America/New_York")

        try:
            open_price = hist_1m.loc[market_open_time]["Open"]
        except:
            print(f"⚠️ Nessun dato alle 09:30 per {ticker}, skippo...")
            continue

        data = fundamentals.copy()
        data.update({
            "Date": day,
            "Open": round(open_price, 2),
            "Prev Close": round(prev_close, 2) if prev_close else None,
            "High": round(row["High"], 2),
            "Low": round(row["Low"], 2),
            "Close": round(row["Close"], 2),
            "Volume": int(row["Volume"]),
            "Open (Daily)": round(row["Open"], 2)
        })

        # Gap%
        if prev_close and open_price:
            gap_pct = round(((open_price - prev_close) / prev_close) * 100, 2)
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

        # 🔹 Prezzi a orari specifici
        target_times = ["10:30", "11:00", "12:00", "14:00"]
        for t in target_times:
            ts = pd.Timestamp(datetime.combine(day, datetime.strptime(t, "%H:%M").time()), tz="America/New_York")
            try:
                close_price = hist_1m.loc[ts]["Close"]
                data[f"Close_{t}"] = round(close_price, 2)
            except KeyError:
                closest_idx = hist_1m.index.get_indexer([ts], method="backfill")[0]
                if closest_idx != -1:
                    close_price = hist_1m.iloc[closest_idx]["Close"]
                    data[f"Close_{t}"] = round(close_price, 2)
                else:
                    data[f"Close_{t}"] = None

        # Intraday (solo 09:30–16:00)
        intraday_market = hist_1m.between_time("09:30", "16:00")
        print(f"🎯 Intraday rows (09:30–16:00): {len(intraday_market)}")

        if len(intraday_market) < 30:
            print(f"❌ Troppi pochi dati intraday per {ticker}, skippo aggregazione...")
            continue

        market_open = market_open_time

        # 🔹 Cumulativi: 30m, 60m, 90m, 4h
        for label, minutes in [("30m", 30), ("1h", 60), ("90m", 90), ("4h", 240)]:
            block_end = market_open + pd.Timedelta(minutes=minutes)
            df_cut = intraday_market[(intraday_market.index >= market_open) & (intraday_market.index < block_end)]

            if df_cut.empty:
                data[f"High_{label}"] = None
                data[f"Low_{label}"] = None
                data[f"Volume_{label}"] = None
                data[f"Break_PMH_{label}"] = "n/a"
                continue

            high = df_cut["High"].max()
            low = df_cut["Low"].min()
            vol = df_cut["Volume"].sum()

            data[f"High_{label}"] = round(high, 2)
            data[f"Low_{label}"] = round(low, 2)
            data[f"Volume_{label}"] = int(vol)
            data[f"Break_PMH_{label}"] = (
                "si" if data.get("High Pre-Market") and high > data["High Pre-Market"] else "no"
            )
            print(f"📊 {ticker} - {label} | High: {high:.2f}, Low: {low:.2f}, Volume: {vol:,}")

        # 🔹 Resample "classico" per 1m, 5m
        for label, resample_rule in resample_map.items():
            try:
                agg = intraday_market.resample(resample_rule, origin=market_open, label="right", closed="right").agg({
                    "High": "max",
                    "Low": "min",
                    "Volume": "sum"
                }).dropna()

                if agg.empty:
                    continue

                first_row = agg.iloc[0]
                high = first_row["High"]
                low = first_row["Low"]
                vol = first_row["Volume"]

                data[f"High_{label}"] = round(high, 2)
                data[f"Low_{label}"] = round(low, 2)
                data[f"Volume_{label}"] = int(vol)

                if data["High Pre-Market"] is not None:
                    data[f"Break_PMH_{label}"] = "si" if high > data["High Pre-Market"] else "no"
                else:
                    data[f"Break_PMH_{label}"] = "n/a"

                print(f"📊 {ticker} - {label} | High: {high:.2f}, Low: {low:.2f}, Volume: {vol:,}")

            except Exception as e:
                print(f"⚠️ Errore aggregazione {ticker} - {label}: {e}")
                data[f"High_{label}"] = None
                data[f"Low_{label}"] = None
                data[f"Volume_{label}"] = None
                data[f"Break_PMH_{label}"] = "n/a"

        # Filtro finale: Volume 1m
        vol_1m = data.get("Volume_1m")
        if vol_1m is not None and vol_1m != 0 and vol_1m < 700_000:
            print("❌ Volume 1m presente e < 700k, skippo...")
            continue

        final_rows.append(data)

# 📤 Salva file Excel
df_final = pd.DataFrame(final_rows)
output_path = f"output/dati_azioni_completo_{date_str}.xlsx"
df_final.to_excel(output_path, index=False)
print(f"✅ File salvato: {output_path}")
