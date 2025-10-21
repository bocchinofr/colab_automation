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

# 🔹 Filtri screener
filters_dict = {
    "Market Cap.": "-Small (under $2bln)",
    "Gap": "Up 20%",
    "Price": "Over $1"
}

# 🔹 Esegue screener sulla vista tecnica (per includere la colonna 'Gap')
technical = Technical()
technical.set_filter(filters_dict=filters_dict)
df_screen = technical.screener_view()

if df_screen is not None and not df_screen.empty:
    # 🔹 Normalizza e moltiplica la colonna 'Gap%'
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

    # 🔹 Aggiunge colonne fondamentali
    shs_float_list = []
    shs_outstand_list = []
    insider_own_list = []
    inst_own_list = []
    short_float_list = []

    for ticker in df_screen["Ticker"]:
        try:
            stock = finvizfinance(ticker)
            stock_fundament = stock.ticker_fundament()

            # Recupero dei campi fondamentali
            shs_float = stock_fundament.get("Shs Float", None)
            shs_outstand = stock_fundament.get("Shs Outstand", None)
            insider_own = stock_fundament.get("Insider Own", None)
            inst_own = stock_fundament.get("Inst Own", None)
            short_float = stock_fundament.get("Short Float", None)

            # Aggiunta alle liste
            shs_float_list.append(shs_float)
            shs_outstand_list.append(shs_outstand)
            insider_own_list.append(insider_own)
            inst_own_list.append(inst_own)
            short_float_list.append(short_float)

        except Exception as e:
            print(f"⚠️ Errore con {ticker}: {e}")
            shs_float_list.append(None)
            shs_outstand_list.append(None)
            insider_own_list.append(None)
            inst_own_list.append(None)
            short_float_list.append(None)

    # 🔹 Aggiunge le nuove colonne al DataFrame
    df_screen["Shs Float"] = shs_float_list
    df_screen["Shares Outstanding"] = shs_outstand_list
    df_screen["Insider Ownership"] = insider_own_list
    df_screen["Institutional Ownership"] = inst_own_list
    df_screen["Short Float"] = short_float_list

    # 🔹 Rimuove colonne indesiderate
    columns_to_drop = ["Beta", "ATR", "SMA20", "SMA50", "SMA200", "52W High", "52W Low", "RSI"]
    df_screen = df_screen.drop(columns=[c for c in columns_to_drop if c in df_screen.columns])

    # 🔹 Salva il risultato finale
    df_screen.to_csv(output_file, index=False)
    print(f"✅ Salvato con colonne pulite e fondamentali: {output_file}")
else:
    print("⚠️ Nessun ticker trovato.")