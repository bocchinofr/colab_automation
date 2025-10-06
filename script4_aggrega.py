import pandas as pd
from datetime import datetime, time, timedelta
import os

# === Percorsi ===
today = datetime.now()
date_str = today.strftime("%Y-%m-%d")
input_path = f"output/intraday/dati_intraday1m_{date_str}.xlsx"
output_dir = "output/intraday"
output_path = os.path.join(output_dir, f"riepilogo_intraday_{date_str}.xlsx")

os.makedirs(output_dir, exist_ok=True)
print(f"📄 Leggo file: {input_path}")

# === Carica dati ===
try:
    df = pd.read_excel(input_path)
except FileNotFoundError:
    raise FileNotFoundError(f"❌ File non trovato: {input_path}")

# Normalizza colonne e datetime
if "Unnamed: 0" in df.columns:
    df = df.rename(columns={"Unnamed: 0": "Datetime"})
df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
df = df.dropna(subset=["Datetime"]).copy()
df["Date"] = df["Datetime"].dt.date
df["Time"] = df["Datetime"].dt.time
df = df.rename(columns={c: c.strip().capitalize() for c in df.columns})

if "Ticker" not in df.columns:
    raise ValueError("Colonna 'Ticker' mancante nel file.")

# forza tipi numerici per sicurezza
for col in ["Open","High","Low","Close","Volume"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

tickers = df["Ticker"].dropna().unique()
print(f"📈 Trovati {len(tickers)} ticker.")

# helper: prendi il primo bucket (index 0) rispetto a rh_start_dt
def first_bucket_stats(rh_df, rh_start_dt, m):
    """
    rh_df: DataFrame con Datetime colonne, ordinato
    rh_start_dt: datetime del market open (es. 2025-10-02 09:30:00)
    m: minuti del bucket (int)
    ritorna (high, low, volume) del primo bucket; None se non presenti
    """
    if rh_df.empty:
        return None, None, 0

    # calcola numero di bucket (delta) per ogni riga
    delta_seconds = (rh_df["Datetime"] - rh_start_dt).dt.total_seconds()
    # bucket index (0 = primo bucket 09:30 .. <09:30+m)
    bucket = (delta_seconds // (m * 60)).astype("Int64")  # può risultare negativo se riga < open (non dovrebbe)
    rh_df = rh_df.assign(_bucket=bucket)
    # prendi solo bucket 0 (fallback al primo bucket esistente se 0 non esiste)
    grouped = rh_df.groupby("_bucket").agg({"High":"max","Low":"min","Volume":"sum"})
    if grouped.empty:
        return None, None, 0

    if 0 in grouped.index:
        g = grouped.loc[0]
    else:
        # fallback: prendi il primo bucket disponibile (ad esempio se non ci sono righe esattamente >=9:30)
        g = grouped.iloc[0]

    high = g["High"] if pd.notnull(g["High"]) else None
    low = g["Low"] if pd.notnull(g["Low"]) else None
    vol = int(g["Volume"]) if pd.notnull(g["Volume"]) else 0
    return high, low, vol

# === Elaborazione per ogni ticker ===
final_data = []
intervals = [1, 5, 30, 60, 90, 240]

for ticker in tickers:
    dft = df[df["Ticker"] == ticker].copy()
    if dft.empty:
        continue

    # tieni solo la data più recente per il ticker
    max_date = dft["Date"].max()
    # day_df è solo le righe del giorno target (per rh)
    day_df = dft[dft["Date"] == max_date].copy()

    # definisci finestre temporali precise (datetime)
    rh_start_dt = datetime.combine(max_date, time(9,30))
    rh_end_dt   = datetime.combine(max_date, time(15,59))
    pm_start_dt = datetime.combine(max_date - timedelta(days=1), time(16,0))
    pm_end_dt   = datetime.combine(max_date, time(9,29))

    # pm_df: prendiamo SOLO le righe comprese tra pm_start_dt e pm_end_dt
    pm_df = dft[(dft["Datetime"] >= pm_start_dt) & (dft["Datetime"] <= pm_end_dt)].sort_values("Datetime").copy()
    # escludi le barre 04:00 e 04:01 se presenti
    pm_df = pm_df[~pm_df["Datetime"].dt.strftime("%H:%M").isin(["04:00", "04:01"])]

    # rh_df: righe nel regular hours del giorno target
    rh_df = day_df[(day_df["Datetime"] >= rh_start_dt) & (day_df["Datetime"] <= rh_end_dt)].sort_values("Datetime").copy()

    # se non ci sono dati né in rh né in pm saltalo
    if rh_df.empty and pm_df.empty:
        continue

    row = {"Ticker": ticker, "Date": max_date}

    # --- Regular hours ---
    if not rh_df.empty:
        # Open: preferisci la barra delle 09:30 altrimenti la prima disponibile
        if any(rh_df["Datetime"] == rh_start_dt):
            open_v = rh_df.loc[rh_df["Datetime"] == rh_start_dt, "Open"].iloc[0]
        else:
            open_v = rh_df["Open"].iloc[0]
        high_v = rh_df["High"].max() if not rh_df["High"].isna().all() else None
        low_v  = rh_df["Low"].min()  if not rh_df["Low"].isna().all() else None
        close_v = rh_df.loc[rh_df["Datetime"] == rh_end_dt, "Close"].iloc[-1] if any(rh_df["Datetime"] == rh_end_dt) else rh_df["Close"].iloc[-1]
        vol_v = int(rh_df["Volume"].sum()) if "Volume" in rh_df.columns else 0

        row["Open"] = round(open_v, 2) if pd.notnull(open_v) else None
        row["High"] = round(high_v, 2) if pd.notnull(high_v) else None
        row["Low"]  = round(low_v, 2)  if pd.notnull(low_v)  else None
        row["Close"]= round(close_v, 2) if pd.notnull(close_v) else None
        row["Volume"] = vol_v
    else:
        row.update({"Open": None, "High": None, "Low": None, "Close": None, "Volume": 0})

    # --- Pre-market ---
    if not pm_df.empty:
        # OpenPM: preferisci la barra esatta alle 16:00 del giorno precedente altrimenti la prima riga nella finestra
        if any(pm_df["Datetime"] == pm_start_dt):
            openpm = pm_df.loc[pm_df["Datetime"] == pm_start_dt, "Open"].iloc[0]
        else:
            openpm = pm_df["Open"].iloc[0]
        highpm = pm_df["High"].max() if not pm_df["High"].isna().all() else None
        lowpm  = pm_df["Low"].min()  if not pm_df["Low"].isna().all() else None
        closepm = pm_df.loc[pm_df["Datetime"] == pm_end_dt, "Close"].iloc[-1] if any(pm_df["Datetime"] == pm_end_dt) else pm_df["Close"].iloc[-1]
        volpm = int(pm_df["Volume"].sum()) if "Volume" in pm_df.columns else 0

        row["OpenPM"] = round(openpm, 2) if pd.notnull(openpm) else None
        row["HighPM"] = round(highpm, 2) if pd.notnull(highpm) else None
        row["LowPM"]  = round(lowpm, 2)  if pd.notnull(lowpm)  else None
        row["ClosePM"]= round(closepm, 2) if pd.notnull(closepm) else None
        row["VolumePM"] = volpm

        # TimePMH: prendi il datetime della prima occorrenza del valore HighPM
        try:
            high_pm_value = highpm
            # filtra le righe che hanno quel valore e ordina per Datetime crescente -> prendi la prima
            high_pm_rows = pm_df[pm_df["High"] == high_pm_value].sort_values("Datetime")
            if not high_pm_rows.empty:
                row["TimePMH"] = high_pm_rows.iloc[0]["Datetime"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                row["TimePMH"] = None
        except Exception:
            row["TimePMH"] = None
    else:
        row.update({"OpenPM": None, "HighPM": None, "LowPM": None, "ClosePM": None, "VolumePM": 0, "TimePMH": None})

    # --- Aggregazioni intraday: prendo il PRIMO bucket che parte alle 09:30 per ogni m ---
    for m in intervals:
        h, l, v = None, None, 0
        if not rh_df.empty:
            # calcola primo bucket rispetto a rh_start_dt
            high_b, low_b, vol_b = first_bucket_stats(rh_df, rh_start_dt, m)
            h, l, v = high_b, low_b, vol_b
        # arrotonda prezzi
        row[f"High_{m}m"] = round(h, 2) if pd.notnull(h) else None
        row[f"Low_{m}m"]  = round(l, 2) if pd.notnull(l) else None
        row[f"Volume_{m}m"] = int(v) if v is not None else 0

    final_data.append(row)

# === Output finale e ordinamento colonne ===
df_final = pd.DataFrame(final_data)

cols_prior = [
    "Ticker", "Date",
    "Open", "High", "Low", "Close", "Volume",
    "OpenPM", "HighPM", "LowPM", "ClosePM", "VolumePM", "TimePMH"
]
# poi per ogni m: High, Low, Volume (nell'ordine richiesto)
for m in intervals:
    cols_prior += [f"High_{m}m", f"Low_{m}m", f"Volume_{m}m"]

# assicurati di avere solo colonne presenti
cols_present = [c for c in cols_prior if c in df_final.columns]
df_final = df_final[cols_present]

df_final.to_excel(output_path, index=False)
print(f"✅ File riepilogativo salvato: {output_path}")
