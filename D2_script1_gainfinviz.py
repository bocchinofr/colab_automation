# D2_script1_gainfinviz.py
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance
import pandas as pd
from datetime import datetime
import os

# 📂 Cartella di output
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# 📅 Data odierna
date_str = datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(output_dir, f"gainers_{date_str}.csv")

# 🔹 Filtri screener Finviz
filters_dict = {
    "Market Cap.": "-Small (under $2bln)",   # Small cap under 2B
    "Change from Open": "Up 20%",                      # Prende tutti con gain > 20%
    "Current Volume": "Over 300K"            # Volume > 0.3 milioni
}

print("🔍 Avvio scansione Finviz...")
print(f"📊 Filtri: Market Cap < 2B, Gain > 20%, Volume > 5M")

# 🔹 Screener overview
foverview = Overview()
foverview.set_filter(filters_dict=filters_dict)
df_screen = foverview.screener_view()

if df_screen is not None and not df_screen.empty:
    print(f"✅ Trovati {len(df_screen)} ticker con gain > 20%")
    
    # 🔹 Estrai percentuale gain
    if "Change from Open" in df_screen.columns:
        df_screen["Gain_%"] = (
            df_screen["Change from Open"]
            .astype(str)
            .str.replace("%", "")
            .str.replace("+", "")
            .astype(float)
        )
        
        # 🔹 FILTRO: teniamo solo gain > 50%
        df_screen = df_screen[df_screen["Gain_%"] > 0.3]
        print(f"📈 Di cui con gain > 50%: {len(df_screen)} ticker")
    
    if df_screen.empty:
        print("⚠️ Nessun ticker con gain superiore al 50% oggi")
    else:
        # 🔹 Pulisci Volume
        if "Volume" in df_screen.columns:
            df_screen["Volume"] = (
                df_screen["Volume"]
                .astype(str)
                .str.replace(",", "")
                .astype(float)
                .astype(int)
            )
        
        # 🔹 Recupera dati aggiuntivi (Short Float, Insider Own, etc.)
        print("📊 Recupero dati fondamentali...")
        
        shs_float_list = []
        shs_outstand_list = []  # 🆕 NUOVA LISTA per Shares Outstanding
        insider_own_list = []
        inst_own_list = []
        short_float_list = []
        
        for idx, ticker in enumerate(df_screen["Ticker"], 1):
            try:
                stock = finvizfinance(ticker)
                stock_fundament = stock.ticker_fundament()
                
                shs_float_list.append(stock_fundament.get("Shs Float"))
                shs_outstand_list.append(stock_fundament.get("Shs Outstand"))  # 🆕 Recupera Shares Outstanding
                insider_own_list.append(stock_fundament.get("Insider Own"))
                inst_own_list.append(stock_fundament.get("Inst Own"))
                short_float_list.append(stock_fundament.get("Short Float"))
                
                print(f"  [{idx}/{len(df_screen)}] {ticker}", end="\r")
                
            except Exception as e:
                print(f"\n⚠️ Errore {ticker}: {e}")
                shs_float_list.append(None)
                shs_outstand_list.append(None)  # 🆕 Aggiungi None in caso di errore
                insider_own_list.append(None)
                inst_own_list.append(None)
                short_float_list.append(None)
        
        print("\n")
        
        # 🔹 Aggiungi colonne
        df_screen["Shs Float"] = shs_float_list
        df_screen["Shares Outstanding"] = shs_outstand_list  # 🆕 NUOVA COLONNA
        df_screen["Insider Own"] = insider_own_list
        df_screen["Inst Own"] = inst_own_list
        df_screen["Short Float"] = short_float_list
        
        # 🔹 Rimuovi colonne inutili
        cols_to_drop = ["Beta", "ATR", "SMA20", "SMA50", "SMA200", "52W High", "52W Low", "RSI", "Change from Open"]
        cols_to_drop = [c for c in cols_to_drop if c in df_screen.columns]
        if cols_to_drop:
            df_screen = df_screen.drop(columns=cols_to_drop)
        
        # 🔹 Ordina per gain
        df_screen = df_screen.sort_values("Gain_%", ascending=False)
        
        # 🔹 Salva CSV
        df_screen.to_csv(output_file, index=False)
        
        print(f"✅ Salvato: {output_file}")
        print(f"📊 Totale ticker con gain > 50%: {len(df_screen)}")
        
        # 🔹 Mostra anteprima
        print("\n🏆 TOP 10:")
        cols_show = ["Ticker", "Price", "Gain_%", "Volume", "Market Cap", "Shares Outstanding"]  # 🆕 Aggiunta Shares Outstanding
        print(df_screen[cols_show].head(10).to_string(index=False))
        
else:
    print("⚠️ Nessun ticker trovato. Prova a ridurre i filtri (es. Volume > 1M)")