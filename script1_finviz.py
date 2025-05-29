from finvizfinance.screener.overview import Overview
import pandas as pd
from datetime import datetime
import os

output_dir = "daily_tickers"
os.makedirs(output_dir, exist_ok=True)

today = datetime.now()
date_str = today.strftime("%Y-%m-%d")

filters_dict = {
    "Market Cap.": "-Small (under $2bln)",
    "Gap": "Up 20%",
    "Price": "Over $1"
}

print("📡 Scarico ticker dallo screener...")
overview = Overview()
overview.set_filter(filters_dict=filters_dict)
df_screen = overview.screener_view()

if df_screen is not None and not df_screen.empty:
    output_file = os.path.join(output_dir, f"tickers_{date_str}.csv")
    df_screen.to_csv(output_file, index=False)
    print(f"✅ {len(df_screen)} ticker salvati in {output_file}")
else:
    print("⚠️ Nessun ticker trovato.")