# D2_script2_yfinance_1m.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

# ------------------------
# 📅 Configurazione date
# ------------------------
today = datetime.now()
yesterday = today - timedelta(days=1)
intraday_date = yesterday

date_str = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")
intraday_date_str = intraday_date.strftime("%Y-%m-%d")

print(f"📅 Oggi: {date_str}")
print(f"📂 Leggo file D1: {yesterday_str}")
print(f"🎯 Recupero dati intraday per: {intraday_date_str}")

# ------------------------
# 🔧 Funzioni di conversione
# ------------------------
def convert_finviz_number(value):
    """Converte stringhe tipo '101.31M' o '1.5B' in interi"""
    if pd.isna(value) or value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        return int(value) if not pd.isna(value) else None
    value_str = str(value).strip().upper()
    try:
        if value_str.endswith('B'):
            return int(float(value_str[:-1]) * 1_000_000_000)
        elif value_str.endswith('M'):
            return int(float(value_str[:-1]) * 1_000_000)
        elif value_str.endswith('K'):
            return int(float(value_str[:-1]) * 1_000)
        else:
            return int(float(value_str))
    except (ValueError, TypeError):
        return None

def parse_percent(value):
    """Converte stringhe tipo '12.5%' in float"""
    if pd.isna(value) or value is None or value == '':
        return None
    try:
        return float(str(value).replace('%', '').replace('+', '').strip())
    except (ValueError, TypeError):
        return None

# ------------------------
# 📥 Legge file gainers (D2_script1)
# ------------------------
input_dir = "output"
gainers_file = os.path.join(input_dir, f"gainers_{yesterday_str}.csv")
gappers_file = os.path.join(input_dir, f"tickers_{yesterday_str}.csv")

df_gainers = pd.DataFrame()
df_gappers = pd.DataFrame()

if os.path.exists(gainers_file):
    df_gainers = pd.read_csv(gainers_file, keep_default_na=False)
    print(f"✅ Gainers caricati: {len(df_gainers)} ticker")
else:
    print(f"⚠️ File gainers non trovato: {gainers_file}")

if os.path.exists(gappers_file):
    df_gappers = pd.read_csv(gappers_file, keep_default_na=False)
    print(f"✅ Gappers caricati: {len(df_gappers)} ticker")
else:
    print(f"⚠️ File gappers non trovato: {gappers_file}")

if df_gainers.empty and df_gappers.empty:
    print("❌ Nessun file trovato. Uscita.")
    exit(1)

# ------------------------
# 🔀 Unifica e deduplica
# ------------------------
# Normalizza colonne % prima del merge
for col in ["Change from Open", "Change", "Gap"]:
    for df in [df_gainers, df_gappers]:
        if col in df.columns:
            df[col] = df[col].apply(parse_percent)

# Rinomina colonne D1 nei due dataframe prima di unire
# Gainers: ha Change from Open, Change, Gap
# Gappers: ha Gap%, Change from Open, Change

# Allinea nomi colonne gappers → gainers
if not df_gappers.empty:
    if "Gap%" in df_gappers.columns and "Gap" not in df_gappers.columns:
        df_gappers = df_gappers.rename(columns={"Gap%": "Gap"})
    if "Insider Ownership" in df_gappers.columns:
        df_gappers = df_gappers.rename(columns={"Insider Ownership": "Insider Own"})
    if "Institutional Ownership" in df_gappers.columns:
        df_gappers = df_gappers.rename(columns={"Institutional Ownership": "Inst Own"})

# Colonne comuni da tenere
common_cols = ["Ticker", "Price", "Change from Open", "Change", "Gap",
               "Volume", "Market Cap", "Shs Float", "Shares Outstanding",
               "Insider Own", "Inst Own", "Short Float",
               "Sector", "Industry", "Country"]

def align_df(df, source_label):
    """Mantieni solo le colonne disponibili e aggiungi source"""
    cols = [c for c in common_cols if c in df.columns]
    out = df[cols].copy()
    out["_source"] = source_label
    return out

df_g1 = align_df(df_gainers, "gainer") if not df_gainers.empty else pd.DataFrame()
df_g2 = align_df(df_gappers, "gapper") if not df_gappers.empty else pd.DataFrame()

# Unisci — gainers hanno priorità in caso di duplicato
df_combined = pd.concat([df_g1, df_g2], ignore_index=True)
df_combined = df_combined.drop_duplicates(subset=["Ticker"], keep="first")

# Conversioni numeriche
for col in ['Shs Float', 'Shares Outstanding', 'Market Cap', 'Volume']:
    if col in df_combined.columns:
        df_combined[col] = df_combined[col].apply(convert_finviz_number)

tickers = df_combined['Ticker'].dropna().unique().tolist()
finviz_map = df_combined.set_index('Ticker').to_dict('index')

print(f"\n📊 Ticker unici da analizzare: {len(tickers)}")
gainers_set = set(df_gainers['Ticker'].dropna().tolist()) if not df_gainers.empty else set()
gappers_set = set(df_gappers['Ticker'].dropna().tolist()) if not df_gappers.empty else set()
print(f"   - Solo gainers: {len(gainers_set - gappers_set)}")
print(f"   - Solo gappers: {len(gappers_set - gainers_set)}")
print(f"   - In entrambi (deduplicati): {len(gainers_set & gappers_set)}")

# ------------------------
# Lista per risultati finali
# ------------------------
final_rows = []

for ticker in tickers:
    print(f"\n📈 Analizzo: {ticker}")
    stock = yf.Ticker(ticker)

    fm = finviz_map.get(ticker, {})

    # ------------------------
    # Dati fondamentali + colonne D1
    # ------------------------
    fundamentals = {
        "Ticker": ticker,
        "Market Cap": fm.get("Market Cap"),
        "d1_change_from_open": fm.get("Change from Open"),
        "d1_change": fm.get("Change"),
        "d1_gap": fm.get("Gap"),
        "Price_D1": fm.get("Price"),
        "Volume_D1": fm.get("Volume"),
        "Shs Float": fm.get("Shs Float"),
        "Insider Own": fm.get("Insider Own"),
        "Inst Own": fm.get("Inst Own"),
        "Short Float": fm.get("Short Float"),
        "Shares Outstanding": fm.get("Shares Outstanding"),
        "Sector": fm.get("Sector"),
        "Industry": fm.get("Industry"),
        "Country": fm.get("Country"),
        "D1_Source": fm.get("_source")
    }

    # ------------------------
    # Dati intraday 1m
    # ------------------------
    try:
        start_date = intraday_date_str
        end_date = (intraday_date + timedelta(days=1)).strftime('%Y-%m-%d')

        hist_1m = stock.history(
            start=start_date,
            end=end_date,
            interval="1m",
            prepost=True
        )

        if hist_1m.empty:
            print(f"⚠️ Nessun dato per {ticker}")
            continue

        if hist_1m.index.tz is None:
            hist_1m.index = hist_1m.index.tz_localize("UTC").tz_convert("America/New_York")
        else:
            hist_1m.index = hist_1m.index.tz_convert("America/New_York")

        hist_1m = hist_1m[hist_1m.index.date == intraday_date.date()]

        if hist_1m.empty:
            print(f"⚠️ Nessun dato per {ticker} nella data {intraday_date_str}")
            continue

        # Pre-Market
        pre_market = hist_1m.between_time("04:00", "09:30").copy()
        last_pm_close = None

        if not pre_market.empty:
            last_pm_close = pre_market.sort_index().iloc[-1]["Close"]

        pre_market.index = pre_market.index.tz_localize(None)
        max_pre = pre_market["High"].max() if not pre_market.empty else None

        for ts, row in pre_market.iterrows():
            data = fundamentals.copy()
            data.update({
                "Datetime": ts.strftime('%Y-%m-%d %H:%M:%S'),
                "Session": "Pre-Market",
                "Open": round(row["Open"], 2),
                "High": round(row["High"], 2),
                "Low": round(row["Low"], 2),
                "Close": round(row["Close"], 2),
                "Volume": int(row["Volume"]),
                "Max Pre-Market": round(max_pre, 2) if max_pre else None
            })
            final_rows.append(data)

        # Regular Market
        regular_market = hist_1m.between_time("09:30", "16:00").copy()
        regular_market.index = regular_market.index.tz_localize(None)

        if last_pm_close is not None and not regular_market.empty:
            first_idx = regular_market.index[0]
            regular_market.loc[first_idx, "Open"] = last_pm_close

        for ts, row in regular_market.iterrows():
            data = fundamentals.copy()
            data.update({
                "Datetime": ts.strftime('%Y-%m-%d %H:%M:%S'),
                "Session": "Regular",
                "Open": round(row["Open"], 2),
                "High": round(row["High"], 2),
                "Low": round(row["Low"], 2),
                "Close": round(row["Close"], 2),
                "Volume": int(row["Volume"]),
                "Max Pre-Market": round(max_pre, 2) if max_pre else None
            })
            final_rows.append(data)

        print(f"✅ {ticker} [{fm.get('_source','?')}] - {len(pre_market)} PM, {len(regular_market)} REG")

    except Exception as e:
        print(f"⚠️ Errore {ticker}: {e}")
        continue

# ------------------------
# Salva
# ------------------------
output_dir = "output/intraday"
os.makedirs(output_dir, exist_ok=True)

df_final = pd.DataFrame(final_rows)
output_path = os.path.join(output_dir, f"D2_gainers_1myfinance_{date_str}.xlsx")
df_final.to_excel(output_path, index=False)

print(f"\n✅ File salvato: {output_path}")
print(f"📊 Totale righe: {len(df_final)}")
print(f"📈 Ticker: {df_final['Ticker'].nunique()}")