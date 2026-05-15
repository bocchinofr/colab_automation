# D2_riepilogo_intraday.py
import pandas as pd
from datetime import datetime, time, timedelta
import os

# region ==== Percorsi ===
today = datetime.now()
date_str = today.strftime("%Y-%m-%d")
yesterday = today - timedelta(days=1)

# Legge il file generato da D2_gainers_intraday.py (che ha già i dati popolati)
input_path = f"output/intraday/D2_gainers_1myfinance.xlsx"
output_dir = "output/intraday"
output_path = os.path.join(output_dir, f"D2_riepilogo_intraday_{date_str}.xlsx")

os.makedirs(output_dir, exist_ok=True)
print(f"📄 Leggo file intraday: {input_path}")
# endregion

# region === Carica dati intraday ===
try:
    df = pd.read_excel(input_path)
    print(f"✅ File caricato: {len(df)} righe")
    print(f"📊 Colonne disponibili: {df.columns.tolist()}")
except FileNotFoundError:
    raise FileNotFoundError(f"❌ File non trovato: {input_path}")

# Standardizza nomi colonne (prima lettera maiuscola, resto minuscolo)
df.columns = [c[0].upper() + c[1:].lower() if isinstance(c, str) else c for c in df.columns]

# Rinomina colonne specifiche se necessario
rename_map = {
    "Gain_%": "Gain_%",
    "Price_gain_giorno": "Price_Gain_Giorno",
    "Volume_gain_giorno": "Volume_Gain_Giorno",
    "Market cap": "Market Cap",
    "Short float": "Short Float",
    "Insider own": "Insider Own",
    "Inst own": "Inst Own",
    "Float shares": "Float Shares",
    "Shares outstanding": "Shares Outstanding"
}
for old, new in rename_map.items():
    if old in df.columns and old != new:
        df = df.rename(columns={old: new})

# Verifica colonne essenziali
essential_cols = ["Ticker", "Datetime", "Open", "High", "Low", "Close", "Volume", "Session"]
missing_cols = [c for c in essential_cols if c not in df.columns]
if missing_cols:
    print(f"⚠️ Colonne mancanti: {missing_cols}")

# Prepara dataframe
df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
df = df.dropna(subset=["Datetime"]).copy()
df["Date"] = df["Datetime"].dt.date
df["Time"] = df["Datetime"].dt.time

for col in ["Open", "High", "Low", "Close", "Volume"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

tickers = df["Ticker"].dropna().unique()
print(f"📈 Trovati {len(tickers)} ticker intraday.")
# endregion

# region === Estrai dati fondamentali dal file intraday ===
# Prendi i valori univoci per ticker (prima riga)
fundamentals_dict = {}
for ticker in tickers:
    dft = df[df["Ticker"] == ticker]
    if not dft.empty:
        first_row = dft.iloc[0]
        
        # Gestisci Gain_% (se è 0.523, moltiplica per 100)
        gain_raw = first_row.get("Gain_%", None)
        if gain_raw is not None and pd.notna(gain_raw):
            if gain_raw < 10:  # Probabilmente è in formato decimale (0.523)
                gain_pct = round(gain_raw * 100)
            else:
                gain_pct = round(gain_raw)
        else:
            gain_pct = None
        
        fundamentals_dict[ticker] = {
            "Market Cap": first_row.get("Market Cap", None),
            "Gain_%": gain_pct,
            "Price_Gain_Giorno": first_row.get("Price_Gain_Giorno", None),
            "Volume_Gain_Giorno": first_row.get("Volume_Gain_Giorno", None),
            "Short Float": first_row.get("Short Float", None),
            "Insider Own": first_row.get("Insider Own", None),
            "Inst Own": first_row.get("Inst Own", None),
            "Float Shares": first_row.get("Float Shares", None),
            "Shares Outstanding": first_row.get("Shares Outstanding", None)
        }

# Debug
print("\n🔍 Verifica dati estratti (primo ticker):")
first_ticker = tickers[0] if len(tickers) > 0 else None
if first_ticker:
    print(f"   Ticker: {first_ticker}")
    for k, v in fundamentals_dict[first_ticker].items():
        print(f"   {k}: {v}")
# endregion

# === Funzioni di calcolo ===
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

def calc_vwap(df):
    df = df.copy()
    df["TypicalPrice"] = (df["High"] + df["Low"] + df["Close"]) / 3
    df["TPxVol"] = df["TypicalPrice"] * df["Volume"]
    df["Cumulative_TPxVol"] = df["TPxVol"].cumsum()
    df["Cumulative_Volume"] = df["Volume"].cumsum()
    df["VWAP"] = df["Cumulative_TPxVol"] / df["Cumulative_Volume"]
    return df

# === Calcolo metriche intraday ===
final_data = []
intervals = [1, 5, 15, 30, 45, 60, 90, 120, 240]

time_targets = {
    "0930": time(9, 30), "1000": time(10, 0), "1030": time(10, 30),
    "1100": time(11, 0), "1130": time(11, 30), "1200": time(12, 0),
    "1230": time(12, 30), "1300": time(13, 0), "1330": time(13, 30),
    "1400": time(14, 0), "1430": time(14, 30), "1500": time(15, 0),
    "1530": time(15, 30), "1600": time(16, 0)
}

for ticker in tickers:
    dft = df[df["Ticker"] == ticker].copy()
    if dft.empty:
        continue

    max_date = dft["Date"].max()
    day_df = dft[dft["Date"] == max_date].copy()

    rh_start_dt = datetime.combine(max_date, time(9, 30))
    rh_end_dt = datetime.combine(max_date, time(15, 59))
    pm_start_dt = datetime.combine(max_date - timedelta(days=1), time(16, 0))
    pm_end_dt = datetime.combine(max_date, time(9, 29))

    # Filtra Pre-Market e Regular Session
    pm_df = dft[(dft["Datetime"] >= pm_start_dt) & (dft["Datetime"] <= pm_end_dt)].copy()
    pm_df = pm_df[~pm_df["Datetime"].dt.strftime("%H:%M").isin(["04:00", "04:01"])]

    if "Session" in day_df.columns:
        rh_df = day_df[
            (day_df["Datetime"] >= rh_start_dt) &
            (day_df["Datetime"] <= rh_end_dt) &
            (day_df["Session"].str.contains("Regular", case=False, na=False))
        ].copy()
    else:
        rh_df = day_df[
            (day_df["Datetime"] >= rh_start_dt) &
            (day_df["Datetime"] <= rh_end_dt)
        ].copy()

    # Correggi Open 09:30 Regular con Open Pre-Market 09:30
    if "Session" in dft.columns and not rh_df.empty:
        pm_930 = dft[
            (dft["Datetime"].dt.time == time(9, 30)) &
            (dft["Session"].str.contains("Pre-Market", case=False, na=False))
        ]
        if not pm_930.empty:
            open_930 = pm_930.iloc[0]["Open"]
            first_idx = rh_df[rh_df["Datetime"] == rh_start_dt].index
            if len(first_idx) > 0:
                rh_df.loc[first_idx[0], "Open"] = open_930

    if rh_df.empty and pm_df.empty:
        continue

    # Prendi i dati fondamentali
    fund_data = fundamentals_dict.get(ticker, {})
    
    # Calcola GAP% (variazione tra Price_Gain_Giorno e Open di oggi)
    price_gain_day = fund_data.get("Price_Gain_Giorno")
    open_today = None
    
    if not rh_df.empty:
        open_today = rh_df.loc[rh_df["Datetime"] == rh_start_dt, "Open"].iloc[0] \
            if any(rh_df["Datetime"] == rh_start_dt) else rh_df["Open"].iloc[0]
    
    gap_pct = None
    if price_gain_day and open_today and price_gain_day > 0:
        gap_pct = ((open_today - price_gain_day) / price_gain_day) * 100

    # Costruisci riga
    row = {
        "Ticker": ticker,
        "Date": max_date,
        "Market Cap": fund_data.get("Market Cap"),
        "Gain_%": fund_data.get("Gain_%"),
        "Price_Gain_Giorno": price_gain_day,
        "Volume_Gain_Giorno": fund_data.get("Volume_Gain_Giorno"),
        "GAP_%": round(gap_pct, 2) if gap_pct else None,
        "Short Float": fund_data.get("Short Float"),
        "Insider Own": fund_data.get("Insider Own"),
        "Inst Own": fund_data.get("Inst Own"),
        "Float Shares": fund_data.get("Float Shares"),
        "Shares Outstanding": fund_data.get("Shares Outstanding")
    }

    # --- Calcolo VWAP ---
    if not rh_df.empty:
        rh_df_vwap = calc_vwap(rh_df)
        target_dt = datetime.combine(max_date, time(9, 30))
        vwap_row = rh_df_vwap.iloc[(rh_df_vwap["Datetime"] - target_dt).abs().argsort()[:1]]
        row["VWAP_0930"] = round(vwap_row["VWAP"].iloc[0], 2) if not vwap_row.empty else None
    else:
        row["VWAP_0930"] = None

    # --- Regular hours ---
    if not rh_df.empty:
        open_v = open_today if open_today else (rh_df.loc[rh_df["Datetime"] == rh_start_dt, "Open"].iloc[0] 
                 if any(rh_df["Datetime"] == rh_start_dt) else rh_df["Open"].iloc[0])
        high_v, low_v, close_v = rh_df["High"].max(), rh_df["Low"].min(), rh_df["Close"].iloc[-1]

        try:
            high_rows = rh_df[rh_df["High"] == high_v].sort_values("Datetime")
            time_high = high_rows.iloc[0]["Datetime"].strftime("%Y-%m-%d %H:%M:%S") if not high_rows.empty else None
        except Exception:
            time_high = None

        try:
            low_rows = rh_df[rh_df["Low"] == low_v].sort_values("Datetime")
            time_low = low_rows.iloc[0]["Datetime"].strftime("%Y-%m-%d %H:%M:%S") if not low_rows.empty else None
        except Exception:
            time_low = None

        row.update({
            "Open": round(open_v, 2),
            "High": round(high_v, 2),
            "Low": round(low_v, 2),
            "Close": round(close_v, 2),
            "TimeHigh": time_high,
            "TimeLow": time_low
        })
        
        # Volume totale Regular (da 09:31 in poi)
        rh_vol_df = rh_df[rh_df["Datetime"] > datetime.combine(max_date, time(9, 30))].copy()
        row["Volume"] = int(rh_vol_df["Volume"].sum()) if not rh_vol_df.empty else 0
    else:
        row.update({"Open": None, "High": None, "Low": None, "Close": None, "Volume": 0, "TimeHigh": None, "TimeLow": None})
        rh_vol_df = pd.DataFrame()

    # --- Pre-market ---
    if not pm_df.empty:
        volpm_row = dft[dft["Datetime"].dt.time == time(9, 30)]
        volpm = int(volpm_row["Volume"].iloc[0]) if not volpm_row.empty else 0

        openpm = pm_df.iloc[0]["Open"]
        highpm, lowpm, closepm = pm_df["High"].max(), pm_df["Low"].min(), pm_df["Close"].iloc[-1]
        row.update({
            "OpenPM": round(openpm, 2),
            "HighPM": round(highpm, 2),
            "LowPM": round(lowpm, 2),
            "ClosePM": round(closepm, 2),
            "VolumePM": volpm
        })
        try:
            high_pm_rows = pm_df[pm_df["High"] == highpm].sort_values("Datetime")
            row["TimePMH"] = high_pm_rows.iloc[0]["Datetime"].strftime("%Y-%m-%d %H:%M:%S") if not high_pm_rows.empty else None
        except Exception:
            row["TimePMH"] = None
    else:
        row.update({"OpenPM": None, "HighPM": None, "LowPM": None, "ClosePM": None, "VolumePM": 0, "TimePMH": None})

    # --- Bucket aggregations ---
    for m in intervals:
        if not rh_df.empty:
            h, l, _ = first_bucket_stats(rh_df, rh_start_dt, m)
        else:
            h, l = None, None

        if not rh_vol_df.empty:
            _, _, v = first_bucket_stats(rh_vol_df, rh_start_dt, m)
        else:
            v = 0

        row[f"High_{m}m"] = round(h, 2) if pd.notnull(h) else None
        row[f"Low_{m}m"] = round(l, 2) if pd.notnull(l) else None
        row[f"Volume_{m}m"] = int(v)
        
        # Close a X minuti
        if not rh_df.empty:
            target_dt = rh_start_dt + timedelta(minutes=m)
            close_row = rh_df.iloc[(rh_df["Datetime"] - target_dt).abs().argsort()[:1]]
            row[f"Close_{m}m"] = round(close_row["Close"].iloc[0], 2) if not close_row.empty else None
        else:
            row[f"Close_{m}m"] = None

    # --- Close per orari specifici ---
    if not rh_df.empty:
        for label, t in time_targets.items():
            target_dt = datetime.combine(max_date, t)
            close_row = rh_df.iloc[(rh_df["Datetime"] - target_dt).abs().argsort()[:1]]
            row[f"Close_{label}"] = round(close_row["Close"].iloc[0], 2) if not close_row.empty else None
    else:
        for label in time_targets.keys():
            row[f"Close_{label}"] = None

    final_data.append(row)

df_final = pd.DataFrame(final_data)

# === Filtro Open > 1$ ===
if "Open" in df_final.columns:
    original_len = len(df_final)
    df_final = df_final[df_final["Open"] > 1].copy()
    print(f"✅ Filtrati: {len(df_final)} ticker (Open > 1$) da {original_len}")

# === Riordino colonne ===
intraday_blocks = []
for m in intervals:
    intraday_blocks.extend([f"High_{m}m", f"Low_{m}m", f"Volume_{m}m", f"Close_{m}m"])

close_time_cols = [f"Close_{label}" for label in time_targets.keys()]

cols_fixed = [
    "Ticker", "Date",
    "Market Cap", "Gain_%", "Price_Gain_Giorno", "Volume_Gain_Giorno", "GAP_%",
    "Short Float", "Insider Own", "Inst Own", "Float Shares", "Shares Outstanding",
    "VWAP_0930",
    "Open", "High", "Low", "Close", "Volume",
    "TimeHigh", "TimeLow",
    "OpenPM", "HighPM", "LowPM", "ClosePM", "VolumePM", "TimePMH"
]

final_columns = [c for c in cols_fixed if c in df_final.columns]
final_columns += [c for c in intraday_blocks if c in df_final.columns]
final_columns += [c for c in close_time_cols if c in df_final.columns]

df_final = df_final[final_columns]

# Salva
df_final.to_excel(output_path, index=False)
print(f"\n✅ File riepilogativo salvato: {output_path}")
print(f"📊 Totale ticker: {len(df_final)}")

# Anteprima
print("\n🔍 Anteprima prime righe:")
preview_cols = ["Ticker", "Gain_%", "Price_Gain_Giorno", "GAP_%", "Open", "Close"]
preview_cols = [c for c in preview_cols if c in df_final.columns]
if preview_cols:
    print(df_final[preview_cols].head(10).to_string(index=False))