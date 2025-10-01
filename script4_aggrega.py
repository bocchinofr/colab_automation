import pandas as pd
from datetime import datetime

# 📥 File di input
today = datetime.now()
date_str = today.strftime("%Y-%m-%d")
input_csv = f"output/intraday1m_all_{date_str}.csv"
print(f"📥 Carico il file: {input_csv}")

# 🔹 Leggi CSV e forza tipi numerici
df = pd.read_csv(input_csv, names=["date","open","high","low","close","volume","ticker"])

# Converte colonne numeriche a float
for col in ['open','high','low','close','volume']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# 🔹 Converti in datetime
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# 🔹 Ordina per ticker e data
df = df.sort_values(by=['ticker','date'])

# 🔹 Format numeri a 2 decimali
df['open'] = df['open'].map(lambda x: round(x,2))
df['high'] = df['high'].map(lambda x: round(x,2))
df['low'] = df['low'].map(lambda x: round(x,2))
df['close'] = df['close'].map(lambda x: round(x,2))
df['volume'] = df['volume'].map(lambda x: round(x,2))

# 💾 Salva CSV pulito
output_csv = f"output/intraday_clean_{date_str}.csv"
df.to_csv(output_csv, index=False)
print(f"✅ File pulito salvato: {output_csv}")
