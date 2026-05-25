from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance
import pandas as pd
from datetime import datetime
import os
import time

output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

date_str = datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(output_dir, f"gainers_{date_str}.csv")

filters_dict = {
    "Market Cap.": "-Small (under $2bln)",
    "Change from Open": "Up 20%",
    "Current Volume": "Over 300K"
}

print("🔍 Avvio scansione Finviz...")

foverview = Overview()
foverview.set_filter(filters_dict=filters_dict)
df_screen = foverview.screener_view()

if df_screen is not None and not df_screen.empty:
    print(f"✅ Trovati {len(df_screen)} ticker")
    print(f"📋 Colonne disponibili: {list(df_screen.columns)}")  # DEBUG: vedi colonne reali

    # FIX 1: cerca la colonna gain con nomi alternativi
    gain_col = None
    for candidate in ["Change from Open", "Change", "Perf Month", "Perf Week"]:
        if candidate in df_screen.columns:
            gain_col = candidate
            break

    if gain_col:
        df_screen["Gain_%"] = (
            df_screen[gain_col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace("+", "", regex=False)
            .str.strip()
        )
        df_screen["Gain_%"] = pd.to_numeric(df_screen["Gain_%"], errors="coerce")
        df_screen = df_screen[df_screen["Gain_%"] > 0.3]
        print(f"📈 Ticker con gain > 0.3%: {len(df_screen)}")
    else:
        print("⚠️ Colonna gain non trovata — procedo senza filtro gain")
        df_screen["Gain_%"] = None  # colonna placeholder per non crashare dopo

    if df_screen.empty:
        print("⚠️ Nessun ticker dopo il filtro")
    else:
        if "Volume" in df_screen.columns:
            df_screen["Volume"] = (
                df_screen["Volume"]
                .astype(str)
                .str.replace(",", "", regex=False)
            )
            df_screen["Volume"] = pd.to_numeric(df_screen["Volume"], errors="coerce").fillna(0).astype(int)

        print("📊 Recupero dati fondamentali...")

        shs_float_list = []
        shs_outstand_list = []
        insider_own_list = []
        inst_own_list = []
        short_float_list = []

        tickers = list(df_screen["Ticker"])

        for idx, ticker in enumerate(tickers, 1):
            try:
                stock = finvizfinance(ticker)
                stock_fundament = stock.ticker_fundament()

                shs_float_list.append(stock_fundament.get("Shs Float"))
                shs_outstand_list.append(stock_fundament.get("Shs Outstand"))
                insider_own_list.append(stock_fundament.get("Insider Own"))
                inst_own_list.append(stock_fundament.get("Inst Own"))
                short_float_list.append(stock_fundament.get("Short Float"))

                print(f"  [{idx}/{len(tickers)}] {ticker} ✓")

            except Exception as e:
                print(f"\n⚠️ Errore {ticker}: {e}")
                shs_float_list.append(None)
                shs_outstand_list.append(None)
                insider_own_list.append(None)
                inst_own_list.append(None)
                short_float_list.append(None)

            # FIX 2: delay per evitare 429
            time.sleep(1.5)

        df_screen["Shs Float"] = shs_float_list
        df_screen["Shares Outstanding"] = shs_outstand_list
        df_screen["Insider Own"] = insider_own_list
        df_screen["Inst Own"] = inst_own_list
        df_screen["Short Float"] = short_float_list

        cols_to_drop = ["Beta", "ATR", "SMA20", "SMA50", "SMA200", "52W High", "52W Low", "RSI"]
        cols_to_drop = [c for c in cols_to_drop if c in df_screen.columns]
        if cols_to_drop:
            df_screen = df_screen.drop(columns=cols_to_drop)

        # FIX 3: sort solo se la colonna è numerica e non tutta None
        if "Gain_%" in df_screen.columns and df_screen["Gain_%"].notna().any():
            df_screen = df_screen.sort_values("Gain_%", ascending=False)

        df_screen.to_csv(output_file, index=False)
        print(f"\n✅ Salvato: {output_file}")
        print(f"📊 Totale ticker: {len(df_screen)}")

        cols_show = [c for c in ["Ticker", "Price", "Gain_%", "Volume", "Market Cap", "Shares Outstanding"] if c in df_screen.columns]
        print("\n🏆 TOP 10:")
        print(df_screen[cols_show].head(10).to_string(index=False))

else:
    print("⚠️ Nessun ticker trovato. Riduci i filtri.")