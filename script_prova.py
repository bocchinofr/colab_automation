import requests
import pandas as pd
from datetime import datetime
import os

# 🔑 API Key Alphavantage
API_KEY = "4T18CQ9W52B3P8OF"

# 🎯 Ticker da scaricare
ticker = input("ANGX").upper()

# 📥 Chiamata API Alpha Vantage (intraday 1m, mercato USA)
url = "https://www.alphavantage.co/query"
params = {
    "function": "TIME_SERIES_INTRADAY",
    "symbol": ticker,
    "interval": "1min",   # puoi cambiare: 1min, 5min, 15min, 30min, 60min
    "apikey": API_KEY,
    "datatype": "json",
    "outputsize": "full"  # "compact" = ultime 100 barre, "full" = fino a 30 giorni
}

print(f"\n⏳ Scarico dati intraday 1m per {ticker} da Alpha Vantage...")
response = requests.get(url, params=params)

if response.status_code != 200:
    print("❌ Errore nella richiesta API")
else:
    data = response.json()

    if "Time Series (1min)" not in data:
        print("⚠️ Nessun dato trovato o limite API raggiunto.")
    else:
        # 📊 Converte in DataFrame
        df = pd.DataFrame.from_dict(data["Time Series (1min)"], orient="index")
        df = df.rename(columns={
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close",
            "5. volume": "Volume"
        })

        # ✅ Converti indici a datetime + ordina
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

        # 📂 Crea cartella se non esiste
        os.makedirs("output", exist_ok=True)

        # 💾 Salva su file Excel
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_path = f"output/dati_intraday1m_{ticker}_{date_str}.xlsx"
        df.to_excel(output_path, index=True)

        print(f"✅ File salvato: {output_path}")
        print(df.head())
