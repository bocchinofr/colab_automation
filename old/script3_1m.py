# script_alpha_pre_market.py
import requests
import pandas as pd
from datetime import datetime, time
import os
import sys

# 🔑 API Key Alpha Vantage
API_KEY = "4T18CQ9W52B3P8OF"

# 📂 Percorsi
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, "output")
intraday_dir = os.path.join(output_dir, "intraday")
os.makedirs(intraday_dir, exist_ok=True)

# 📅 Data odierna
date_str = datetime.now().strftime("%Y-%m-%d")
file_tickers = os.path.join(output_dir, f"tickers_{date_str}.csv")

# Se viene passato un parametro da linea di comando, lo usa come suffisso
# altrimenti default = "premarket"
suffix = sys.argv[1] if len(sys.argv) > 1 else "premarket"

# 📄 Carica ticker filtrati da Finviz
df_tickers = pd.read_csv(file_tickers)
tickers = df_tickers["Ticker"].dropna().unique().tolist()
print(f"📊 Trovati {len(tickers)} ticker dal CSV: {file_tickers}")

# 📘 DataFrame finale
all_data = pd.DataFrame()

# ⏰ Intervallo pre-market (04:00 - 09:30 ET)
# Nota: Alpha Vantage restituisce timestamp in ET già formattato come HH:MM:SS
start_time = time(4, 0)
end_time = time(9, 30)

for ticker in tickers:
    print(f"\n⏳ Scarico dati intraday 1m per {ticker}...")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": ticker,
        "interval": "1min",
        "apikey": API_KEY,
        "datatype": "json",
        # "outputsize": "full"  # rimosso per free API
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "Time Series (1min)" not in data:
            print(f"⚠️ Nessun dato trovato o limite API raggiunto per {ticker}.")
            continue

        df = pd.DataFrame.from_dict(data["Time Series (1min)"], orient="index")
        df = df.rename(columns={
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close",
            "5. volume": "Volume"
        })

        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

        # ⏱️ Filtra per intervallo pre-market
        df = df[(df.index.time >= start_time) & (df.index.time <= end_time)]

        if df.empty:
            print(f"⚠️ Nessun dato disponibile per {ticker} nell'intervallo selezionato.")
            continue

        df["Ticker"] = ticker
        all_data = pd.concat([all_data, df])

    except Exception as e:
        print(f"❌ Errore con {ticker}: {e}")

# 💾 Salva unico file Excel
if not all_data.empty:
    output_path = os.path.join(intraday_dir, f"dati_intraday1m_{suffix}_{date_str}.xlsx")
    all_data.to_excel(output_path, index=True)
    print(f"\n✅ File salvato: {output_path}")
else:
    print("\n⚠️ Nessun dato scaricato per i ticker selezionati.")
