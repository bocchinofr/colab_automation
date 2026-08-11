# D2_script1_gainfinviz.py
from finvizfinance.screener.technical import Technical
import yfinance as yf
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
    "Market Cap.": "-Small (under $2bln)",
    "Change from Open": "Up 20%",
    "Current Volume": "Over 300K"
}

print("🔍 Avvio scansione Finviz...")
print(f"📊 Filtri: Market Cap < 2B, Change from Open > 20%, Volume > 300K")

# 🔹 Screener tecnico (espone Gap e Change from Open)
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
    print(f"✅ Trovati {len(df_screen)} ticker con Change from Open > 20%")

    # 🔹 Pulisci Volume
    if "Volume" in df_screen.columns:
        df_screen["Volume"] = (
            df_screen["Volume"]
            .astype(str)
            .str.replace(",", "")
            .astype(float)
            .astype(int)
        )

    # 🔹 Normalizza Change from Open (rimuovi %)
    for col in ["Change from Open", "Change", "Gap"]:
        if col in df_screen.columns:
            df_screen[col] = (
                df_screen[col]
                .astype(str)
                .str.replace("%", "")
                .str.replace("+", "")
                .str.strip()
            )
            df_screen[col] = pd.to_numeric(df_screen[col], errors="coerce")

    # 🔹 Recupera dati fondamentali
    print("📊 Recupero dati fondamentali...")

    shs_float_list = []
    shs_outstand_list = []
    insider_own_list = []
    inst_own_list = []
    short_float_list = []
    sector_list = []
    industry_list = []
    country_list = []
    market_cap_list = []

    for idx, ticker in enumerate(df_screen["Ticker"], 1):
        try:
            info = yf.Ticker(ticker).info

            market_cap = info.get("marketCap")
            float_shares = info.get("floatShares")
            shares_outstanding = info.get("sharesOutstanding")
            insider_own = info.get("heldPercentInsiders")
            inst_own = info.get("heldPercentInstitutions")
            short_float = info.get("shortPercentOfFloat")

            # Settore / Industry / Country
            sector = info.get("sector")
            industry = info.get("industry")
            country = info.get("country")

            # Controllo coerenza Float / Outstanding
            if (
                float_shares is not None and
                shares_outstanding is not None and
                float_shares > shares_outstanding * 1.05
            ):
                print(
                    f"\n⚠️ {ticker}: FLOAT anomalo "
                    f"({float_shares:,}) > OUTSTANDING ({shares_outstanding:,}) "
                    f"-> Float ignorato"
                )
                float_shares = None

            shs_float_list.append(float_shares)
            shs_outstand_list.append(shares_outstanding)
            insider_own_list.append(insider_own)
            inst_own_list.append(inst_own)
            short_float_list.append(short_float)
            sector_list.append(sector)
            industry_list.append(industry)
            country_list.append(country)
            market_cap_list.append(market_cap)

            print(
                f"  [{idx}/{len(df_screen)}] "
                f"{ticker} | MC={market_cap} | FLOAT={float_shares}",
                end="\r"
            )

        except Exception as e:
            print(f"\n⚠️ Errore {ticker}: {e}")
            shs_float_list.append(None)
            shs_outstand_list.append(None)
            insider_own_list.append(None)
            inst_own_list.append(None)
            short_float_list.append(None)
            sector_list.append(None)
            industry_list.append(None)
            country_list.append(None)
            market_cap_list.append(None)

    print("\n")

    # 🔹 Aggiungi colonne fondamentali
    df_screen["Shs Float"] = shs_float_list
    df_screen["Shares Outstanding"] = shs_outstand_list
    df_screen["Insider Own"] = insider_own_list
    df_screen["Inst Own"] = inst_own_list
    df_screen["Short Float"] = short_float_list
    df_screen["Sector"] = sector_list
    df_screen["Industry"] = industry_list
    df_screen["Country"] = country_list
    df_screen["Market Cap"] = market_cap_list

    # 🔹 Rimuovi colonne inutili (Change NON rimosso)
    cols_to_drop = ["Beta", "ATR", "SMA20", "SMA50", "SMA200", "52W High", "52W Low", "RSI"]
    cols_to_drop = [c for c in cols_to_drop if c in df_screen.columns]
    if cols_to_drop:
        df_screen = df_screen.drop(columns=cols_to_drop)

    # 🔹 Ordina per Change from Open
    if "Change from Open" in df_screen.columns:
        df_screen = df_screen.sort_values("Change from Open", ascending=False)

    # 🔹 Salva CSV
    df_screen.to_csv(output_file, index=False)

    print(f"✅ Salvato: {output_file}")
    print(f"📊 Totale ticker: {len(df_screen)}")

    # 🔹 Anteprima
    print("\n🏆 TOP 10:")
    cols_show = [c for c in ["Ticker", "Price", "Change from Open", "Change", "Gap", "Volume", "Market Cap", "Shares Outstanding"] if c in df_screen.columns]
    print(df_screen[cols_show].head(10).to_string(index=False))

else:
    print("⚠️ Nessun ticker trovato. Prova a ridurre i filtri.")
