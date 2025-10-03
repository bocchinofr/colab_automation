# script3_intraday_1m_yesterday.py
import pandas as pd
import time
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime, timedelta
import pytz
import os

# 🔑 Chiave Alpha Vantage
ALPHA_VANTAGE_KEY = "4T18CQ9W52B3P8OF"

# 📥 Cerca automaticamente il file tickers più recente in cartella
ticker_file = None
ticker_files = [f for f in os.listdir("output") if f.startswith("tickers_") and f.endswith(".csv")]
if ticker_files:
    ticker_files.sort(reverse=True)  # ordina per data decrescente
    ticker_file = os.path.join("output", ticker_files[0])  # prende il più recente
    print(f"📂 Uso file tickers: {ticker_file}")
else:
    raise FileNotFoundError("⚠️ Nessun file 'tickers_*.csv' trovato in output/")

# 📥 Legge i ticker
df_tickers = pd.read_csv(ticker_file, keep_default_na=False)
tickers = df_tickers['Ticker'].dropna().unique().tolist()
print(f"📊 Ticker trovati: {tickers}")


# 📂 Cartella output
output_folder = "output/aggregati"
os.makedirs(output_folder, exist_ok=True)

# ⏳ Istanza Alpha Vantage
ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas', indexing_type='date')

final_data = []

# Timezone di riferimento (mercato USA, Eastern Time)
ET = pytz.timezone("US/Eastern")

# Oggi (in UTC)
oggi = datetime.now(ET).date()

# Ieri
ieri = oggi - timedelta(days=1)

# Giorno prima di ieri
giorno_prima = oggi - timedelta(days=2)

start_dt = ET.localize(datetime.combine(giorno_prima, datetime.min.time()) + timedelta(hours=16))   # dalle 16:00 di ieri
end_dt   = ET.localize(datetime.combine(ieri, datetime.min.time()) + timedelta(hours=15, minutes=59))  # fino alle 15:59 di ieri


print("📅 Estrazione dati tra", start_dt, "e", end_dt)



# 📥 Scarico dati
for i, ticker in enumerate(tickers, 1):
    print(f"\n⏳ Scarico dati 1m per {ticker}... ({i}/{len(tickers)})")
    try:
        # Recupera dati intraday full
        data, meta = ts.get_intraday(symbol=ticker, interval='1min', outputsize='full')
        print(f"📈 Raw dati {ticker}: {len(data)} righe totali")

        if not data.empty:
            print(data.head(5))  # Mostra le prime 5 righe grezze
        else:
            print(f"⚠️ Nessun dato grezzo da Alpha Vantage per {ticker}")

        # Se ci sono dati, continua elaborazione
        if not data.empty:
            data.index = pd.to_datetime(data.index)
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
    output_path = os.path.join(output_folder, f"intraday1m_all_{oggi.strftime('%Y-%m-%d')}.csv")
    df_all.to_csv(output_path, index=False, float_format="%.4f")
    print(f"\n✅ Tutti i dati salvati in unico CSV: {output_path}")
else:
    print("⚠️ Nessun dato estratto, file non creato.")
