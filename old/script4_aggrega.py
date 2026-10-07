#region === PRE CHECK SCRIPT =====

def preflight_checks_strict(input_path_premarket, input_path_intraday, finviz_path, output_path):
    import os
    import pandas as pd

    print("=== Preflight checks (STRICT) ===")

    # 1️⃣ File essenziali
    essential_files = {
        "Premarket intraday": input_path_premarket,
        "Intraday 1m": input_path_intraday,
        "Finviz": finviz_path
    }

    for desc, path in essential_files.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"❌ File mancante: {desc} → {path}")
        print(f"✅ File trovato: {desc} → {path}")

    # 2️⃣ Cartella output
    out_dir = os.path.dirname(output_path)
    if not os.path.exists(out_dir):
        print(f"⚠️ Cartella output mancante, creo {out_dir}")
        os.makedirs(out_dir, exist_ok=True)
    else:
        print(f"✅ Cartella output esiste → {out_dir}")

    # 3️⃣ Controllo colonne intraday
    required_cols_intraday = ["Ticker","Open","High","Low","Close","Volume","Datetime"]
    for path in [input_path_premarket, input_path_intraday]:
        df_tmp = pd.read_excel(path)
        missing_cols = [c for c in required_cols_intraday if c not in df_tmp.columns]
        if missing_cols:
            raise ValueError(f"❌ File {path} manca colonne essenziali: {missing_cols}")
        print(f"✅ File {path} ha tutte le colonne essenziali")

    # 4️⃣ Colonne necessarie per Float Effettivo in Finviz
    df_finviz = pd.read_csv(finviz_path)
    float_cols = ["Shs Float","Shares Outstanding","Institutional Ownership","Insider Ownership"]
    missing_float = [c for c in float_cols if c not in df_finviz.columns]
    if missing_float:
        raise ValueError(f"❌ Impossibile calcolare Float Effettivo, mancano colonne: {missing_float}")
    print("✅ Tutte le colonne per Float Effettivo presenti")

    # 5️⃣ Test scrittura output
    try:
        pd.DataFrame().to_excel(output_path)
        print(f"✅ Output path scrivibile → {output_path}")
    except Exception as e:
        raise IOError(f"❌ Problema scrittura output: {e}")

    print("✅ Tutti i controlli STRICT superati")
    print("===============================\n")
    return True

# ==== Esempio di utilizzo ====
preflight_checks_strict(input_path_premarket, input_path_intraday, finviz_path, output_path)


# endregion





import pandas as pd
from datetime import datetime, time, timedelta
import os
import re

# region ==== Percorsi ===
today = datetime.now()
date_str = today.strftime("%Y-%m-%d")

input_path_premarket = f"output/intraday/dati_intraday1m_premarket_{date_str}.xlsx"
input_path_intraday  = f"output/intraday/dati_intraday1m_intraday_{date_str}.xlsx"

finviz_path = f"output/tickers_{date_str}.csv"
output_dir = "output/intraday"
output_path = os.path.join(output_dir, f"riepilogo_intraday_{date_str}.xlsx")

os.makedirs(output_dir, exist_ok=True)
print(f"📄 Leggo file intraday: {input_path_premarket}")
print(f"📄 Leggo file intraday: {input_path_intraday}")
# endregion

# region === Carica dati intraday ===

df_list = []
for path in [input_path_premarket, input_path_intraday]:
    if os.path.exists(path):
        tmp_df = pd.read_excel(path)
        if "Unnamed: 0" in tmp_df.columns:
            tmp_df = tmp_df.rename(columns={"Unnamed: 0": "Datetime"})
        tmp_df["Datetime"] = pd.to_datetime(tmp_df["Datetime"], errors="coerce")
        tmp_df = tmp_df.dropna(subset=["Datetime"]).copy()
        tmp_df["Date"] = tmp_df["Datetime"].dt.date
        tmp_df["Time"] = tmp_df["Datetime"].dt.time
        tmp_df = tmp_df.rename(columns={c: c.strip().capitalize() for c in tmp_df.columns})
        df_list.append(tmp_df)
    else:
        print(f"⚠️ File non trovato: {path}")

if not df_list:
    raise FileNotFoundError("❌ Nessun file intraday trovato, interrompo lo script.")

# Unisco i due file
df = pd.concat(df_list, ignore_index=True).sort_values(["Ticker","Datetime"])

tickers = df["Ticker"].dropna().unique()
print(f"📈 Trovati {len(tickers)} ticker intraday.")
# endregion

# region === Carica dati Finviz ===
try:
    df_finviz = pd.read_csv(finviz_path)
    print(f"📊 Letto file Finviz: {finviz_path}")
except FileNotFoundError:
    print(f"⚠️ File Finviz non trovato ({finviz_path}), continuerò senza queste colonne.")
    df_finviz = pd.DataFrame(columns=[
        "Ticker", "Gap%", "Shs Float", "Shares Outstanding", "Change from Open",
        "Insider Ownership", "Institutional Ownership", "Short Float", "Market Cap"
    ])

df_finviz.columns = [c.strip() for c in df_finviz.columns]
cols_finviz = [
    "Ticker", "Gap%", "Shs Float", "Shares Outstanding", "Change from Open",
    "Insider Ownership", "Institutional Ownership", "Short Float", "Market Cap"
]
df_finviz = df_finviz[[c for c in cols_finviz if c in df_finviz.columns]].copy()

# === Funzione per convertire "M", "B", "K" in numeri reali ===
def parse_shares(value):
    if pd.isna(value):
        return None
    s = str(value).replace(",", "").strip().upper()
    match = re.match(r"([\d\.]+)([KMB]?)", s)
    if not match:
        return None
    num, suffix = match.groups()
    num = float(num)
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
    return num * multiplier

# === Applica conversione alle colonne Finviz ===
if "Shs Float" in df_finviz.columns:
    df_finviz["Shs Float"] = df_finviz["Shs Float"].apply(parse_shares)
if "Shares Outstanding" in df_finviz.columns:
    df_finviz["Shares Outstanding"] = df_finviz["Shares Outstanding"].apply(parse_shares)
if "Market Cap" in df_finviz.columns:
    df_finviz["Market Cap"] = df_finviz["Market Cap"].apply(parse_shares)


# === Conversione percentuali in numerico ===
percent_cols = ["Insider Ownership", "Institutional Ownership", "Short Float"]
for col in percent_cols:
    if col in df_finviz.columns:
        df_finviz[col] = (
            df_finviz[col]
            .astype(str)                              # converte tutto in stringa
            .str.replace("%", "", regex=False)         # rimuove il simbolo %
            .replace(["-", "N/A", "nan", ""], None)    # sostituisce i valori non validi
        )
        df_finviz[col] = pd.to_numeric(df_finviz[col], errors="coerce")  # converte in float

# endregion


# region === Recupero dati da Yfinance ====

# === Recupera dati di chiusura giornaliera di ieri da YFinance ===
yesterday = today.date() - timedelta(days=1)

# Lista ticker da usare per YFinance: prendo direttamente quelli trovati nel file intraday
tickers_yf = tickers  # 'tickers' è già definito come df["Ticker"].dropna().unique()

# Dizionario per salvare dati giornalieri
daily_data = {}

for ticker in tickers_yf:
    try:
        data = yf.download(ticker, start=yesterday, end=today.date(), interval="1d", progress=False)
        if not data.empty:
            row = data.iloc[0]
            daily_data[ticker] = {
                "Daily_Open": row["Open"],
                "Daily_High": row["High"],
                "Daily_Low": row["Low"],
                "Daily_Close": row["Close"],
                "Daily_Volume": int(row["Volume"])
            }
        else:
            daily_data[ticker] = {
                "Daily_Open": None,
                "Daily_High": None,
                "Daily_Low": None,
                "Daily_Close": None,
                "Daily_Volume": None
            }
    except Exception as e:
        print(f"⚠️ Errore con {ticker} su YFinance: {e}")
        daily_data[ticker] = {
            "Daily_Open": None,
            "Daily_High": None,
            "Daily_Low": None,
            "Daily_Close": None,
            "Daily_Volume": None
        }

# Trasforma in DataFrame
df_daily = pd.DataFrame.from_dict(daily_data, orient="index").reset_index().rename(columns={"index": "Ticker"})

# Merge con il DataFrame principale
df_merged = pd.merge(df_merged, df_daily, on="Ticker", how="left")

# endregion


# === Funzione per bucket intraday ===
def first_bucket_stats(rh_df, rh_start_dt, m):
    if rh_df.empty:
        return None, None, 0
    delta_seconds = (rh_df["Datetime"] - rh_start_dt).dt.total_seconds()
    bucket = (delta_seconds // (m * 60)).astype("Int64")
    rh_df = rh_df.assign(_bucket=bucket)
    grouped = rh_df.groupby("_bucket").agg({"High": "max", "Low": "min", "Volume": "sum"})
    if grouped.empty:
        return None, None, 0
    g = grouped.loc[0] if 0 in grouped.index else grouped.iloc[0]
    return g["High"], g["Low"], int(g["Volume"])

# === calcolo vwap ====
def calc_vwap(df):
    """Calcola VWAP cumulativo dai dati intraday a 1 minuto"""
    df = df.copy()
    df["TypicalPrice"] = (df["High"] + df["Low"] + df["Close"]) / 3
    df["TPxVol"] = df["TypicalPrice"] * df["Volume"]
    df["Cumulative_TPxVol"] = df["TPxVol"].cumsum()
    df["Cumulative_Volume"] = df["Volume"].cumsum()
    df["VWAP"] = df["Cumulative_TPxVol"] / df["Cumulative_Volume"]
    return df

# === Calcolo metriche intraday ===
final_data = []
intervals = [1, 5, 30, 60, 90, 120, 240]

for ticker in tickers:
    dft = df[df["Ticker"] == ticker].copy()
    if dft.empty:
        continue

    max_date = dft["Date"].max()
    day_df = dft[dft["Date"] == max_date].copy()

    rh_start_dt = datetime.combine(max_date, time(9,30))
    rh_end_dt   = datetime.combine(max_date, time(15,59))
    pm_start_dt = datetime.combine(max_date - timedelta(days=1), time(16,0))
    pm_end_dt   = datetime.combine(max_date, time(9,29))

    pm_df = dft[(dft["Datetime"] >= pm_start_dt) & (dft["Datetime"] <= pm_end_dt)].copy()
    pm_df = pm_df[~pm_df["Datetime"].dt.strftime("%H:%M").isin(["04:00", "04:01"])]

    rh_df = day_df[(day_df["Datetime"] >= rh_start_dt) & (day_df["Datetime"] <= rh_end_dt)].copy()
    if rh_df.empty and pm_df.empty:
        continue
    
    row = {"Ticker": ticker, "Date": max_date}

    if not rh_df.empty:
        rh_df = calc_vwap(rh_df)
        # Calcolo VWAP più vicino alle 09:30
        target_dt = datetime.combine(max_date, time(9,30))
        vwap_row = rh_df.iloc[(rh_df["Datetime"] - target_dt).abs().argsort()[:1]]
        row["VWAP_0930"] = round(vwap_row["VWAP"].iloc[0], 2) if not vwap_row.empty else None
    else:
        row["VWAP_0930"] = None

    # --- Regular hours ---
    if not rh_df.empty:
        open_v = rh_df.loc[rh_df["Datetime"] == rh_start_dt, "Open"].iloc[0] if any(rh_df["Datetime"] == rh_start_dt) else rh_df["Open"].iloc[0]
        high_v, low_v, close_v = rh_df["High"].max(), rh_df["Low"].min(), rh_df["Close"].iloc[-1]
        vol_v = int(rh_df["Volume"].sum())
        
        # TimeHigh
        try:
            high_rows = rh_df[rh_df["High"] == high_v].sort_values("Datetime")
            time_high = high_rows.iloc[0]["Datetime"].strftime("%Y-%m-%d %H:%M:%S") if not high_rows.empty else None
        except Exception:
            time_high = None

        # TimeLow
        try:
            low_rows = rh_df[rh_df["Low"] == low_v].sort_values("Datetime")
            time_low = low_rows.iloc[0]["Datetime"].strftime("%Y-%m-%d %H:%M:%S") if not low_rows.empty else None
        except Exception:
            time_low = None

        row.update({
            "Open": round(open_v,2),
            "High": round(high_v,2),
            "Low": round(low_v,2),
            "Close": round(close_v,2),
            "Volume": vol_v,
            "TimeHigh": time_high,
            "TimeLow": time_low
        })
    else:
        row.update({
            "Open": None, "High": None, "Low": None, "Close": None, "Volume": 0,
            "TimeHigh": None, "TimeLow": None
        })


    # --- Pre-market ---
    if not pm_df.empty:
        openpm = pm_df.iloc[0]["Open"]
        highpm, lowpm, closepm = pm_df["High"].max(), pm_df["Low"].min(), pm_df["Close"].iloc[-1]
        volpm = int(pm_df["Volume"].sum())
        row.update({
            "OpenPM": round(openpm,2),
            "HighPM": round(highpm,2),
            "LowPM": round(lowpm,2),
            "ClosePM": round(closepm,2),
            "VolumePM": volpm
        })
        try:
            high_pm_rows = pm_df[pm_df["High"] == highpm].sort_values("Datetime")
            row["TimePMH"] = high_pm_rows.iloc[0]["Datetime"].strftime("%Y-%m-%d %H:%M:%S") if not high_pm_rows.empty else None
        except Exception:
            row["TimePMH"] = None
    else:
        row.update({"OpenPM": None,"HighPM": None,"LowPM": None,"ClosePM": None,"VolumePM": 0,"TimePMH": None})

    # --- Bucket aggregations ---
    for m in intervals:
        h, l, v = (first_bucket_stats(rh_df, rh_start_dt, m) if not rh_df.empty else (None,None,0))
        row[f"High_{m}m"] = round(h,2) if pd.notnull(h) else None
        row[f"Low_{m}m"]  = round(l,2) if pd.notnull(l) else None
        row[f"Volume_{m}m"] = int(v) if v is not None else 0

    # --- Close a orari precisi ---
    if not rh_df.empty:
        time_targets = {
            "1030": time(10, 30),
            "1100": time(11, 0),
            "1200": time(12, 0),
            "1400": time(14, 0)
        }

        for label, t in time_targets.items():
            target_dt = datetime.combine(max_date, t)

            # Cerca il record più vicino all’orario target
            close_row = rh_df.iloc[(rh_df["Datetime"] - target_dt).abs().argsort()[:1]]

            row[f"Close_{label}"] = (
                round(close_row["Close"].iloc[0], 2)
                if not close_row.empty
                else None
            )
    else:
        for label in ["1030", "1100", "1200", "1400"]:
            row[f"Close_{label}"] = None


    final_data.append(row)

df_final = pd.DataFrame(final_data)

# === Merge con Finviz ===
df_merged = pd.merge(df_final, df_finviz, on="Ticker", how="left")

# === Converti Gap% in numerico ===
df_merged["Gap%"] = pd.to_numeric(df_merged["Gap%"], errors="coerce")

# === Calcolo flottante effettivo ===
if all(col in df_merged.columns for col in ["Shs Float", "Shares Outstanding", "Institutional Ownership", "Insider Ownership"]):
    df_merged["Float Effettivo"] = (
        df_merged["Shs Float"]
        - (df_merged["Institutional Ownership"] / 100 * df_merged["Shares Outstanding"])
        - (df_merged["Insider Ownership"] / 100 * df_merged["Shares Outstanding"])
    )
else:
    print("⚠️ Attenzione: una o più colonne necessarie per il calcolo del flottante effettivo non sono presenti.")

# === FILTRI ESCLUSIVI ===
df_merged = df_merged[
    (df_merged["Gap%"] >= 30) &
    (df_merged["Float Effettivo"] <= 50_000_000)
].copy()


print(f"✅ Filtrati: {len(df_merged)} ticker dopo esclusione Gap<30% o Float>50M")




# === Riordino colonne: TimeHigh, TimeLow e Close a orari precisi ===
cols_start = [
    "Ticker", "Date", "Gap%", "Market Cap", "Shs Float", "Shares Outstanding",
    "Change from Open", "Insider Ownership", "Institutional Ownership", "Short Float", "VWAP_0930"
]
cols_intraday = [c for c in df_final.columns if c not in cols_start]

# Rimuovo TimeHigh, TimeLow e Close se già presenti
for col in ["TimeHigh", "TimeLow", "Close_1030", "Close_1100", "Close_1200", "Close_1400"]:
    if col in cols_intraday:
        cols_intraday.remove(col)

# Mappa per inserire Close dopo i bucket
close_after = {
    "Volume_60m": "Close_1030",
    "Volume_90m": "Close_1100",
    "Volume_120m": "Close_1200",
    "Volume_240m": "Close_1400"
}

cols_intraday_sorted = []
for c in cols_intraday:
    cols_intraday_sorted.append(c)

    # Inserimento TimeHigh/TimeLow dopo Volume
    if c == "Volume":
        cols_intraday_sorted.append("TimeHigh")
        cols_intraday_sorted.append("TimeLow")

    # Inserimento Close dopo il bucket corrispondente
    if c in close_after:
        cols_intraday_sorted.append(close_after[c])

# Combino colonne di testa + resto ordinate
df_merged = df_merged[[c for c in cols_start if c in df_merged.columns] + cols_intraday_sorted]


df_merged.to_excel(output_path, index=False)
print(f"✅ File riepilogativo salvato: {output_path}")
