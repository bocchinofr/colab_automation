# script1_finviz.py
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import Quote
import pandas as pd
from datetime import datetime
import os

# 📂 Cartella output
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# 📅 Nome file con data
date_str = datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(output_dir, f"tickers_{date_str}.csv")

# 🔍 Filtri per screener
filters_dict = {
    "Market Cap.": "-Small (under $2bln)",
    "Gap": "Up 20%",
    "Price": "Over $1"
}

# 📥 Estrazione da Finviz screener
overview = Overview()
overview.set_filter(filters_dict=filters_dict)
df_screen = overview.screener_view()

# ✅ Aggiunta fondamentali (Shs Float, Shs Outstand)
extra_data = []
if df_screen is not None and not df_screen.empty:
    for ticker in df_screen["Ticker"]:
        try:
            q = Quote(ticker)  # ✅ ticker passato al costruttore
            data = q.ticker_fundament()

            shs_float = data.get("Shs Float", None)
            shs_outstand = data.get("Shs Outstand", None)

            extra_data.append({
                "Ticker": ticker,
                "Shs Float": shs_float,
                "Shs Outstand": shs_outstand
            })
        except Exception as e:
            print(f"⚠️ Errore con {ticker}: {e}")
            extra_data.append({
                "Ticker": ticker,
                "Shs Float": None,
