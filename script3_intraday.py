import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# 📅 Calcola le date (stesso blocco di script2)
today = datetime.now()
yesterday = today - timedelta(days=1)
start_date = yesterday.strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")
date_str = yesterday.strftime("%Y-%m-%d")

# 📥 Legge i ticker generati dal primo script (stesso di script2)
ticker_file = f"output/tickers_{end_date}.csv"
df_tickers = pd.read_csv(ticker_file, keep_default_na=False)
tickers = df_tickers['Ticker'].dropna().unique().tolist()

print(f"📊 Ticker trovati: {tickers}")

final_data = {}

for ticker in tickers:
    print(f"\n⏳ Scarico dati 1m per {ticker}...")

    stock = yf.Ticker(ticker)

    # intervallo: dalle 16:00 di ieri (post-market T-1) alle 16:00 di oggi
    start_dt = pd.Timestamp(
        datetime.combine(yesterday.date(), datetime.strptime("16:00", "%H:%M").time()),
        tz="America/New_York"
    )
    end_dt = pd.Timestamp(
        datetime.combine(today.date(), datetime.strptime("16:00", "%H:%M").time()),
        tz="America/New_York"
    )

    hist_1m = stock.history(
        start=start_dt.tz_convert("UTC"),
        end=(end_dt + timedelta(minutes=1)).tz_convert("UTC"),
        interval="1m"
    )

    if hist_1m.empty:
        print(f"⚠️ Nessun dato trovato per {ticker}, skippo...")
        continue

    # ✅ Gestione fuso orario
    if hist_1m.index.tz is None:
        hist_1m.index = hist_1m.index.tz_localize("UTC").tz_convert("America/New_York")
    else:
        hist_1m.index = hist_1m.index.tz_convert("America/New_York")

    hist_1m = hist_1m.sort_index()
    hist_1m = hist_1m[(hist_1m.index >= start_dt) & (hist_1m.index <= end_dt)]

    if hist_1m.empty:
        print(f"⚠️ Nessun dato utile nell’intervallo per {ticker}")
        continue

    # Salvo nel dict
    final_data[ticker] = hist_1m[["Open", "High", "Low", "Close", "Volume"]].copy()

# 📤 Salva file Excel con un foglio per ticker
if final_data:
    output_path = f"output/dati_intraday1m_{date_str}.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for ticker, df in final_data.items():
            df.to_excel(writer, sheet_name=ticker, index=True)
    print(f"✅ File salvato: {output_path}")
else:
    print("⚠️ Nessun dato estratto, file non creato.")
