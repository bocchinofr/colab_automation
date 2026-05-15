# D2_gainers_intraday.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

# ------------------------
# 📅 Configurazione date
# ------------------------
today = datetime.now()
yesterday = today - timedelta(days=1)
intraday_date = yesterday  # I dati intraday sono di ieri (giorno dopo il gain)

date_str = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")
intraday_date_str = intraday_date.strftime("%Y-%m-%d")

print(f"📅 Oggi: {date_str}")
print(f"📂 Leggo file gainers di ieri: {yesterday_str}")
print(f"🎯 Recupero dati intraday per: {intraday_date_str} (giorno dopo il gain)")

# ------------------------
# 📥 Legge ticker dal file gainers di ieri
# ------------------------
input_dir = "output"
ticker_file = os.path.join(input_dir, f"gainers_{yesterday_str}.csv")

if not os.path.exists(ticker_file):
    print(f"❌ File non trovato: {ticker_file}")
    print(f"💡 Assicurati di aver eseguito D2_script1_gainfinviz.py ieri")
    exit(1)

df_tickers = pd.read_csv(ticker_file)
tickers = df_tickers['Ticker'].dropna().unique().tolist()

# Crea un dizionario con i dati Finviz per ogni ticker
finviz_map = df_tickers.set_index('Ticker').to_dict('index')

print(f"📊 Trovati {len(tickers)} ticker nel file gainers_{yesterday_str}.csv")

# ------------------------
# Funzione per convertire valori Finviz
# ------------------------
def parse_finviz_shares(x):
    if x is None or x == '' or pd.isna(x):
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
    # Dati fondamentali (dal file Finviz - relativi al giorno del gain)
    # ------------------------
    fundamentals = {
        "Ticker": ticker,
        "Market Cap": finviz_map.get(ticker, {}).get("Market Cap"),
        "Gain_%": finviz_map.get(ticker, {}).get("Gain_%"),
        "Price_Gain_Giorno": finviz_map.get(ticker, {}).get("Price"),
        "Volume_Gain_Giorno": finviz_map.get(ticker, {}).get("Volume"),
        "Short Float": finviz_map.get(ticker, {}).get("Short Float"),
        "Insider Own": finviz_map.get(ticker, {}).get("Insider Own"),
        "Inst Own": finviz_map.get(ticker, {}).get("Inst Own")
    }
    
    # Opzionale: recupera float shares da yfinance
    try:
        info = stock.info
        fundamentals["Float Shares"] = info.get("floatShares")
        fundamentals["Shares Outstanding"] = info.get("sharesOutstanding")
    except:
        fundamentals["Float Shares"] = None
        fundamentals["Shares Outstanding"] = None

    # ------------------------
    # Dati intraday 1 minuto per il giorno dopo il gain (intraday_date)
    # ------------------------
    try:
        # Scarica i dati per il giorno dopo il gain
        start_date = intraday_date_str
        end_date = (intraday_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        hist_1m = stock.history(
            start=start_date,
            end=end_date,
            interval="1m",
            prepost=True  # include pre-market e after-hours
        )

        if hist_1m.empty:
            print(f"⚠️ Nessun dato intraday 1m per {ticker} in data {intraday_date_str}")
            continue

        # Gestione fuso orario
        if hist_1m.index.tz is None:
            hist_1m.index = hist_1m.index.tz_localize("UTC").tz_convert("America/New_York")
        else:
            hist_1m.index = hist_1m.index.tz_convert("America/New_York")

        # Filtra solo la data target (giorno dopo il gain)
        hist_1m = hist_1m[hist_1m.index.date == intraday_date.date()]

        if hist_1m.empty:
            print(f"⚠️ Nessun dato per {ticker} nella data {intraday_date_str}")
            continue

        # ------------------------
        # Pre-Market: 04:00 - 09:30 ET
        # ------------------------
        pre_market = hist_1m.between_time("04:00", "09:30").copy()
        last_pm_close = None

        if not pre_market.empty:
            last_pm_close = pre_market.sort_index().iloc[-1]["Close"]

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
                "Max Pre-Market": round(max_pre, 2) if max_pre else None
            })
            final_rows.append(data)

        # ------------------------
        # Mercato Regolare: 09:30 - 16:00 ET
        # ------------------------
        regular_market = hist_1m.between_time("09:30", "16:00").copy()
        regular_market.index = regular_market.index.tz_localize(None)

        # Sostituisci l'open del primo minuto con l'ultimo pre-market se disponibile
        if last_pm_close is not None and not regular_market.empty:
            first_idx = regular_market.index[0]
            regular_market.loc[first_idx, "Open"] = last_pm_close

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
                "Max Pre-Market": round(max_pre, 2) if max_pre else None
            })
            final_rows.append(data)

        print(f"✅ {ticker} - {len(pre_market)} righe Pre-Market, {len(regular_market)} righe Regular")

    except Exception as e:
        print(f"⚠️ Errore dati intraday {ticker}: {e}")
        continue

# ------------------------
# Salva file Excel
# ------------------------
output_dir = "output/intraday"
os.makedirs(output_dir, exist_ok=True)

df_final = pd.DataFrame(final_rows)
output_path = os.path.join(output_dir, f"D2_gainers_1myfinance.xlsx")
df_final.to_excel(output_path, index=False)

print(f"\n✅ File salvato: {output_path}")
print(f"📊 Totale righe: {len(df_final)}")
print(f"📈 Ticker processati: {df_final['Ticker'].nunique()}")