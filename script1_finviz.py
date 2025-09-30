# script1_finviz.py (versione aggiornata)

from finvizfinance.screener.overview import Overview
from finvizfinance.quote import Quote
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

overview = Overview()
overview.set_filter(filters_dict=filters_dict)
df_screen = overview.screener_view()

if df_screen is not None and not df_screen.empty:
    # Aggiungi colonne per i fondamentali
    df_screen["Shs Float"] = None
    df_screen["Shs Outstand"] = None

    for i, ticker in enumerate(df_screen["Ticker"]):
        try:
            q = Quote(ticker)
            fundamentals = q.ticker_fundament(raw=True, output_format="dict")
            # fundamentals è un dict con molte chiavi, tra cui "Shs Float" e "Shs Outstand" se disponibili
            shs_float = fundamentals.get("Shs Float")
            shs_out = fundamentals.get("Shs Outstand")
            df_screen.at[i, "Shs Float"] = shs_float
            df_screen.at[i, "Shs Outstand"] = shs_out
            print(f"{ticker} → Float: {shs_float}, Outstand: {shs_out}")
            time.sleep(1)  # optional delay per non stressare finviz
        except Exception as e:
            print(f"Errore con {ticker}: {e}")

    df_screen.to_csv(output_file, index=False)
    print(f"✅ Salvato con fondamentali: {output_file}")
else:
    print("⚠️ Nessun ticker trovato.")
