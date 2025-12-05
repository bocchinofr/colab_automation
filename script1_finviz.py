# script1_finviz.py
from finvizfinance.screener.technical import Technical
from finvizfinance.quote import finvizfinance
import pandas as pd
from datetime import datetime
import os

# 📂 Cartella di output
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# 📅 Data odierna
date_str = datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(output_dir, f"tickers_{date_str}.csv")

# 🔹 Filtri screener Finviz
filters_dict = {
    "Market Cap.": "-Small (under $2bln)",
    "Gap": "Up 20%",
    "Price": "Over $1",
    "Current Volume": "Over 2M",
    "Float": "Under 100M"
}

# 🔹 Screener tecnico (necessario per colonna Gap)
technical = Technical()
technical.set_filter(filters_dict=filters_dict)
df_screen = technical.screener_view()

if df_screen is not None and not df_screen.empty:

    # 🔹 Normalizza Gap%
    if "Gap" in df_screen.columns:
        df_screen["Gap%"] = (
            df_screen["Gap"]
            .astype(str)
            .str.replace("%", "")
            .astype(float)
            .mul(100)
            .round(2)
        )
    else:
        df_screen["Gap%"] = None
        
    # 🔹 Filtro post-estrazione: Gap% > 30%
    df_screen = df_screen[df_screen["Gap%"] > 30]

    # 🔹 Normalizza Volume (rende numerico)
    if "Current Volume" in df_screen.columns:
        df_screen["Volume"] = (
            df_screen["Volume"]
            .astype(str)
            .str.replace(",", "")          # rimuove eventuali virgole
            .astype(float)                 # prima float
            .astype(int)                   # poi int
        )


    # 🔹 Normalizza Float (in milioni)
    if "Float" in df_screen.columns:
        df_screen["Float"] = (
            df_screen["Float"]
            .astype(str)
            .str.replace("M", "")
            .astype(float)
        )

    # 🔹 Recupero fondamentali aggiuntivi
    shs_float_list = []
    shs_outstand_list = []
    insider_own_list = []
    inst_own_list = []
    short_float_list = []
    market_cap_list = []

    for ticker in df_screen["Ticker"]:
        try:
            stock = finvizfinance(ticker)
            stock_fundament = stock.ticker_fundament()

            shs_float_list.append(stock_fundament.get("Shs Float"))
            shs_outstand_list.append(stock_fundament.get("Shs Outstand"))
            insider_own_list.append(stock_fundament.get("Insider Own"))
            inst_own_list.append(stock_fundament.get("Inst Own"))
            short_float_list.append(stock_fundament.get("Short Float"))
            market_cap_list.append(stock_fundament.get("Market Cap"))

        except Exception as e:
            print(f"⚠️ Errore con {ticker}: {e}")
            shs_float_list.append(None)
            shs_outstand_list.append(None)
            insider_own_list.append(None)
            inst_own_list.append(None)
            short_float_list.append(None)
            market_cap_list.append(None)

    # 🔹 Aggiunge nuove colonne
    df_screen["Shs Float"] = shs_float_list
    df_screen["Shares Outstanding"] = shs_outstand_list
    df_screen["Insider Ownership"] = insider_own_list
    df_screen["Institutional Ownership"] = inst_own_list
    df_screen["Short Float"] = short_float_list
    df_screen["Market Cap"] = market_cap_list

    # 🔹 Colonne inutili da rimuovere
    columns_to_drop = ["Beta", "ATR", "SMA20", "SMA50", "SMA200", "52W High", "52W Low", "RSI"]
    df_screen = df_screen.drop(columns=[c for c in columns_to_drop if c in df_screen.columns])

    # 🔹 Salva CSV finale
    df_screen.to_csv(output_file, index=False)
    print(f"✅ Salvato con nuovi filtri e colonne: {output_file}")

else:
    print("⚠️ Nessun ticker trovato.")
