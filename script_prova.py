import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# 🔑 API Key Alpha Vantage
API_KEY = "4T18CQ9W52B3P8OF"

# 📅 Calcola date dinamiche
today = datetime.now()
start_date = (today - timedelta(days=2)).replace(hour=16, minute=0, second=0, microsecond=0)
end_date = (today - timedelta(days=1)).replace(hour=19, minute=50, second=0, microsecond=0)

print(f"📆 Intervallo temporale: da {start_date} a {end_date}")

# 📂 Percorso file ticker (nome dinamico con data)
date_str = datetime.now().strftime("%Y-%m-%d")
file_tickers = f"output/intraday/tickers_{date_str}.csv"

# 📄 Carica lista ticker
if file_tickers.endswith(".csv"):
    df_tickers = pd.read_csv(file_tickers)
elif file_tickers.endswith((".xlsx", ".xls")):
    df_tickers = pd.read_excel(file_tickers, engine="openpyxl")
else:
    raise ValueError("❌ Formato file non supportato. Usa CSV o Excel.")

tickers = df_tickers["Ticker"].dropna().unique().tolist()

print(f"📊 Trovati {len(tickers)} ticker nel file {file_tickers}")

# 📂 Crea cartella output
os.makedirs("output", exist_ok=True)

# 📘 DataFrame finale cumulativo
all_data = pd.DataFrame()

for ticker in tickers:
    print(f"\n⏳ Scarico dati intraday 1m per {ticker} da Alpha Vantage...")
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": ticker,
        "interval": "1min",
        "apikey": API_KEY,
        "datatype": "json",
        "outputsize": "full"
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

        # ⏱️ Filtra per intervallo temporale desiderato
        df = df[(df.index >= start_date) & (df.index <= end_date)]

        if df.empty:
            print(f"⚠️ Nessun dato disponibile per {ticker} nell'intervallo selezionato.")
            continue

        # ➕ Aggiungi colonna Ticker
        df["Ticker"] = ticker

        # 📊 Accumula nel DataFrame finale
        all_data = pd.concat([all_data, df])

    except Exception as e:
        print(f"❌ Errore con {ticker}: {e}")

# 💾 Salva unico file Excel
if not all_data.empty:
    output_path = f"output/dati_intraday1m_{date_str}.xlsx"
    all_data.to_excel(output_path, index=True)
    print(f"\n✅ File unico salvato: {output_path}")
else:
    print("\n⚠️ Nessun dato scaricato per nessun ticker.")
