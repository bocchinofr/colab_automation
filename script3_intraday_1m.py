# script3_intraday_1m.py

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os

# --- Imposta il fuso orario USA ---
ny_tz = pytz.timezone("America/New_York")

# --- Percorsi file ---
input_file = "output/ticker_finviz.csv"  # file generato da script1
today_str = datetime.now(ny_tz).strftime("%Y-%m-%d")
output_file = f"output/dati_intraday_1m_{today_str}.xlsx"

# --- Calcolo intervallo temporale ---
oggi = datetime.now(ny_tz).date()
ieri = oggi - timedelta(days=1)

start = datetime.combine(ieri, datetime.min.time()).replace(hour=16, tzinfo=ny_tz)
end = datetime.combine(oggi, datetime.min.time()).replace(hour=16, tzinfo=ny_tz)

print(f"Intervallo: {start} → {end}")

# --- Carico i ticker ---
if not os.path.exists(input_file):
    raise FileNotFoundError(f"File non trovato: {input_file}")

df_ticker = pd.read_csv(input_file)
tickers = df_ticker['Ticker'].dropna().unique().tolist()

print(f"Trovati {len(tickers)} ticker")

# --- Raccolta dati ---
all_data = []

for ticker in tickers:
    try:
        print(f"Scarico dati per {ticker}...")
        df = yf.download(
            ticker,
            start=start,
            end=end,
            interval="1m",
            prepost=True,
            progress=False
        )

        if df.empty:
            print(f"⚠️ Nessun dato per {ticker}")
            continue

        # Converto timezone in New York
        df.index = df.index.tz_convert("America/New_York")

        # Filtro solo tra le 16:00 di ieri e le 16:00 di oggi
        df = df.loc[(df.index >= start) & (df.index <= end)]

        # Aggiungo ticker
        df["Ticker"] = ticker

        all_data.append(df)

    except Exception as e:
        print(f"Errore con {ticker}: {e}")

# --- Unisco i dati ---
if all_data:
    final_df = pd.concat(all_data)
    final_df.reset_index(inplace=True)
    final_df.rename(columns={"index": "Datetime"}, inplace=True)

    # Riordino colonne
    final_df = final_df[["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

    # Arrotondo a 2 decimali
    for col in ["Open", "High", "Low", "Close"]:
        final_df[col] = final_df[col].round(2)

    # Salvo su Excel
    os.makedirs("output", exist_ok=True)
    final_df.to_excel(output_file, index=False)
    print(f"✅ Dati salvati in {output_file}")
else:
    print("⚠️ Nessun dato disponibile per nessun ticker")
