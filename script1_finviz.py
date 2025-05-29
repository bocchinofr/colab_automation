# script1_finviz.py
from finvizfinance.screener.overview import Overview
import pandas as pd
from datetime import datetime
import os

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
    df_screen.to_csv(output_file, index=False)
    print(f"✅ Salvato: {output_file}")
else:
    print("⚠️ Nessun ticker trovato.")
