# script1_finviz.py
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import Stock
import pandas as pd
from datetime import datetime
import os
import time

output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

date_str = datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(output_dir, f"tickers_{date_str}.csv")

filters_dict = {
    "Market Cap.": "-Small (under $2bln)",
    "Gap": "Up 20%",
    "Price": "Over $1"
}

# 🔹 Screener
overview = Overview()
overview.set_filter(filters_dict=filters_dict)
df_screen = overview.screener_view()

if df_screen is not None and not df_screen.empty:
    # Aggiungiamo colonne vuote
    df_screen["Shs Float"] = None
    df_screen["Shs Outstanding"] = None

    for i, ticker in enumerate(df_screen["Ticker"]):
        try:
            stock = Stock(ticker)
            fundamentals = stock.ticker_fundament()

            shs_float = fundamentals.get("Shs Float")
            shs_out = fundamentals.get("Shs Outstand")

            df_screen.at[i, "Shs Float"] = shs_float
            df_screen.at[i, "Shs Outstanding"] = shs_out

            print(f"📊 {ticker}: Float={shs_float}, Outstand={shs_out}")
            time.sleep(1)  # ⏳ piccolo delay per non stressare Finviz
        except Exception as e:
            print(f"⚠️ Errore {ticker}: {e}")

    # Salva file CSV
    df_screen.to_csv(output_file, index=False)
    print(f"✅ Salvato con fondamentali: {output_file}")

else:
    print("⚠️ Nessun ticker trovato.")
