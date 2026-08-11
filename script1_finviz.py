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
    #"Price": "Over $1",
    "Current Volume": "Over 2M",
    #"Float": "Under 100M"
}

# 🔹 Screener tecnico (necessario per colonna Gap)
technical = Technical()
technical.set_filter(filters_dict=filters_dict)
df_screen = technical.screener_view()

# ════════════════════════════════════════════════════════════
# 🩹 WORKAROUND TEMPORANEO — bug libreria finvizfinance 1.3.0
# Bug noto: la libreria duplica la prima lettera del ticker
# (es. "ELVA" -> "EELVA"), causando 404 su Finviz.
# Segnalato qui: https://github.com/lit26/finvizfinance/issues/158
# Nessuna fix ufficiale rilasciata al 2026-07-16.
#
# ⚠️ RIMUOVERE questo blocco (e i due controlli sotto) non appena
# i maintainer rilasciano una nuova versione che risolve il problema.
# ════════════════════════════════════════════════════════════
if df_screen is not None and not df_screen.empty and "Ticker" in df_screen.columns:

    def fix_duplicated_ticker(t):
        t = str(t).strip()
        if len(t) >= 2 and t[0] == t[1]:
            return t[1:]
        return t

    fixed_tickers = df_screen["Ticker"].apply(fix_duplicated_ticker)
    changed_mask = fixed_tickers != df_screen["Ticker"]
    if changed_mask.any():
        print(f"🔧 [WORKAROUND] Corretti {changed_mask.sum()} ticker duplicati: "
              f"{list(zip(df_screen['Ticker'][changed_mask], fixed_tickers[changed_mask]))}")
    df_screen["Ticker"] = fixed_tickers

    # Scarta ticker chiaramente malformati (parsing rotto) prima di chiamare Finviz
    before_count = len(df_screen)
    df_screen = df_screen[df_screen["Ticker"].str.len().between(1, 5)]
    dropped = before_count - len(df_screen)
    if dropped > 0:
        print(f"🔧 [WORKAROUND] Scartati {dropped} ticker malformati (lunghezza anomala)")
# ════════════════════════════════════════════════════════════
# 🩹 FINE WORKAROUND TEMPORANEO
# ════════════════════════════════════════════════════════════

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

    print("====================================")
    import finvizfinance
    print("VERSIONE FINVIZFINANCE:", finvizfinance.__version__)
    print("TEST FULL INFO")

    test_stock = finvizfinance("AAPL")
    test_info = test_stock.ticker_full_info()

    print(type(test_info))
    print(test_info)

    print("====================================")

    for ticker in df_screen["Ticker"]:

        stock = finvizfinance(ticker)

        try:
            stock_fundament = stock.ticker_fundament()
            print(f"{ticker} -> {stock_fundament}")
        except Exception as e:
            print(f"ERRORE ticker_fundament per {ticker}: {e}")

        break


else:
    print("⚠️ Nessun ticker trovato.")
