import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# ------------------------
# 📅 Configurazione date
# ------------------------
today = datetime.now()
yesterday = today - timedelta(days=1)
date_str = yesterday.strftime("%Y-%m-%d")
date_fin = today.strftime("%Y-%m-%d")

# ------------------------
# 📥 Legge ticker dal file Finviz
# ------------------------
ticker_file = f"output/tickers_{date_fin}.csv"
df_tickers = pd.read_csv(ticker_file, keep_default_na=False)
tickers = df_tickers['Ticker'].dropna().unique().tolist()
finviz_map = df_tickers.set_index('Ticker').to_dict('index')

# ------------------------
# Funzione per convertire valori Finviz
# ------------------------
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

# ------------------------
# Lista per risultati finali
# ------------------------
final_rows = []

for ticker in tickers:
    print(f"\n📈 Analizzo: {ticker}")
    stock = yf.Ticker(ticker)

    # ------------------------
    # Dati fondamentali
    # ------------------------
    try:
        info = stock.info
        yf_float = info.get("floatShares")
        yf_outstanding = info.get("sharesOutstanding")

        finviz_float = parse_finviz_shares(finviz_map.get(ticker, {}).get("Shs Float"))
        finviz_out = parse_finviz_shares(finviz_map.get(ticker, {}).get("Shs Outstanding"))

        float_shares = min([v for v in [yf_float, finviz_float] if v is not None], default=None)
        shares_out = min([v for v in [yf_outstanding, finviz_out] if v is not None], default=None)

        # Filtri: float > 50M
        if float_shares is not None and float_shares > 50_000_000:
            print(f"❌ Float troppo alto ({float_shares}), skippo...")
            continue

        fundamentals = {
            "Ticker": ticker,
            "Market Cap": info.get("marketCap"),
            "Shares Outstanding": shares_out,
            "Float Shares": float_shares
        }

    except Exception as e:
        print(f"⚠️ Errore fondamentali: {e}")
        continue

    # ------------------------
    # Dati intraday 1 minuto (pre-market + mercato regolare)
    # ------------------------
    try:
        hist_1m = stock.history(
            start=yesterday.strftime('%Y-%m-%d'),
            end=(yesterday + timedelta(days=1)).strftime('%Y-%m-%d'),
            interval="1m",
            prepost=True  # include pre-market e after-hours
        )

        if hist_1m.empty:
            print(f"⚠️ Nessun dato intraday 1m per {ticker}")
            continue

        # ✅ Gestione fuso orario
        if hist_1m.index.tz is None:
            hist_1m.index = hist_1m.index.tz_localize("UTC").tz_convert("America/New_York")
        else:
            hist_1m.index = hist_1m.index.tz_convert("America/New_York")

        # ------------------------
        # Filtro volume pre-market tramite primo minuto 09:30
        # ------------------------
        try:
            market_open_time = pd.Timestamp(datetime.combine(yesterday, datetime.strptime("09:30", "%H:%M").time()))
            market_open_time = market_open_time.tz_localize("America/New_York")
            vol_first_minute = hist_1m.loc[market_open_time]["Volume"]
            if vol_first_minute < 2_000_000:
                print(f"❌ {ticker} volume totale pre-market < 2M ({vol_first_minute}), skippo...")
                continue
        except Exception as e:
            print(f"⚠️ Errore volume primo minuto {ticker}: {e}")
            continue

        # ------------------------
        # Pre-Market: 04:00 - 09:30 ET
        # ------------------------
        pre_market = hist_1m.between_time("04:00", "09:30").copy()
        pre_market.index = pre_market.index.tz_localize(None)
        max_pre = pre_market["High"].max() if not pre_market.empty else None

        for ts, row in pre_market.iterrows():
            data = fundamentals.copy()
            data.update({
                "Datetime": ts.strftime('%Y-%m-%d %H:%M:%S'),
                "Session": "Pre-Market",
                "Open": round(row["Open"], 2),
                "High": round(row["High"], 2),
                "Low": round(row["Low"], 2),
                "Close": round(row["Close"], 2),
                "Volume": int(row["Volume"]),
                "Max Pre-Market": max_pre
            })
            final_rows.append(data)

        # ------------------------
        # Mercato Regolare: 09:30 - 16:00 ET
        # ------------------------
        regular_market = hist_1m.between_time("09:30", "16:00").copy()
        regular_market.index = regular_market.index.tz_localize(None)

        for ts, row in regular_market.iterrows():
            data = fundamentals.copy()
            data.update({
                "Datetime": ts.strftime('%Y-%m-%d %H:%M:%S'),
                "Session": "Regular",
                "Open": round(row["Open"], 2),
                "High": round(row["High"], 2),
                "Low": round(row["Low"], 2),
                "Close": round(row["Close"], 2),
                "Volume": int(row["Volume"]),
                "Max Pre-Market": max_pre
            })
            final_rows.append(data)

        print(f"✅ {ticker} - {len(pre_market)} righe Pre-Market, {len(regular_market)} righe Regular salvate")

    except Exception as e:
        print(f"⚠️ Errore dati intraday {ticker}: {e}")
        continue

# ------------------------
# Salva file Excel
# ------------------------
df_final = pd.DataFrame(final_rows)
output_path = f"output/intraday/dati_intraday_1m_yfinance_{date_str}.xlsx"
df_final.to_excel(output_path, index=False)
print(f"✅ File salvato: {output_path}")
