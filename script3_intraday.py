# script3_intraday_1m_yesterday.py
import pandas as pd
import time
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
import pytz
import os

# 🔑 Chiave Alpha Vantage
ALPHA_VANTAGE_KEY = "PR8DXOISAUX28X8N"

# 📥 Legge i ticker generati dallo script 1
ticker_file = "output/tickers_2025-10-01.csv"  # aggiorna con il file corretto
df_tickers = pd.read_csv(ticker_file, keep_default_na=False)
tickers = df_tickers['Ticker'].dropna().unique().tolist()
print(f"📊 Ticker trovati: {tickers}")

# 📂 Cartella output
output_folder = "output"
os.makedirs(output_folder, exist_ok=True)

# ⏳ Istanza Alpha Vantage
ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas', indexing_type='date')

final_data = []

# ⏱️ Definisci intervallo: dalle 20:00 del giorno precedente alle 20:00 di ieri
ny_tz = pytz.timezone("America/New_York")
today = datetime.now(ny_tz).date()
yesterday = today - timedelta(days=1)
start_dt = ny_tz.localize(datetime.combine(yesterday, datetime.min.time().replace(hour=20)))  # 20:00 del giorno precedente
end_dt = ny_tz.localize(datetime.combine(today, datetime.min.time().replace(hour=20)))       # 20:00 di ieri
print(f"📅 Estrazione dati tra {start_dt} e {end_dt}")

# 📥 Scarico dati
for i, ticker in enumerate(tickers, 1):
    print(f"\n⏳ Scarico dati 1m per {ticker}... ({i}/{len(tickers)})")
    try:
        # Recupera dati intraday full
        data, meta = ts.get_intraday(symbol=ticker, interval='1min', outputsize='full')
        data.index = pd.to_datetime(data.index)

        # Converti a timezone NY
        data = data.tz_localize('UTC').tz_convert('America/New_York')

        # Filtra per intervallo tra start_dt e end_dt
        data = data[(data.index >= start_dt) & (data.index <= end_dt)]
        # Rendi naive per comodità
        data.index = data.index.tz_localize(None)

        if data.empty:
            print(f"⚠️ Nessun dato utile per {ticker}, skippo...")
            continue

        # Aggiungi colonna Ticker
        data['Ticker'] = ticker

        # Rinomina colonne in stile CSV finale
        data = data.rename(columns={
            '1. open': 'Open',
            '2. high': 'High',
            '3. low': 'Low',
            '4. close': 'Close',
            '5. volume': 'Volume'
        })

        final_data.append(data)
        print(f"✅ Dati raccolti per {ticker}, righe: {len(data)}")

        # 💤 Pausa per non superare il limite API (5 richieste/min)
        time.sleep(12)

    except Exception as e:
        print(f"⚠️ Errore con {ticker}: {e}")

# 💾 Salva tutto in un unico CSV
if final_data:
    df_all = pd.concat(final_data).reset_index()
    df_all = df_all.rename(columns={'index': 'Date'})
    output_path = os.path.join(output_folder, f"intraday1m_all_{today.strftime('%Y-%m-%d')}.csv")
    df_all.to_csv(output_path, index=False, float_format="%.4f")
    print(f"\n✅ Tutti i dati salvati in unico CSV: {output_path}")
else:
    print("⚠️ Nessun dato estratto, file non creato.")
