# D2_script3_aggrega.py
import pandas as pd
from datetime import datetime, time, timedelta
import os

# region ==== Percorsi ===
today = datetime.now()
date_str = today.strftime("%Y-%m-%d")

# Legge il file generato da D2_script2_yfinance_1m.py
input_path = f"output/intraday/D2_gainers_1myfinance_{date_str}.xlsx"
output_dir = "output/intraday"
output_path = os.path.join(output_dir, f"D2_riepilogo_intraday_{date_str}.xlsx")

os.makedirs(output_dir, exist_ok=True)
print(f"📄 Leggo file intraday: {input_path}")
# endregion

# region === Carica dati intraday ===
try:
    df = pd.read_excel(input_path)
    print(f"✅ File caricato: {len(df)} righe")
except FileNotFoundError:
    raise FileNotFoundError(f"❌ File non trovato: {input_path}")

# Standardizza nomi colonne
#df.columns = [c[0].upper() + c[1:].lower() if isinstance(c, str) else c for c in df.columns]

df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
df = df.dropna(subset=["Datetime"]).copy()
df["Date"] = df["Datetime"].dt.date
df["Time"] = df["Datetime"].dt.time

for col in ["Open", "High", "Low", "Close", "Volume"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

for col in ["Short Float", "Insider Own", "Inst Own"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace("%", "", regex=False).str.strip()
        df[col] = pd.to_numeric(df[col], errors="coerce")

tickers = df["Ticker"].dropna().unique()
print(f"📈 Trovati {len(tickers)} ticker intraday.")
# endregion

# region === Estrai dati fondamentali ===
fundamentals_dict = {}
for ticker in tickers:
    dft = df[df["Ticker"] == ticker]
    if not dft.empty:
        first_row = dft.iloc[0]
        fundamentals_dict[ticker] = {
            "Market Cap": first_row.get("Market Cap", None),
            "d1_change_from_open": first_row.get("d1_change_from_open", None),
            "d1_change": round(first_row.get("d1_change", None) * 100) if first_row.get("d1_change") is not None else None,            
            "d1_gap": round(first_row.get("d1_gap", None) * 100) if first_row.get("d1_gap") is not None else None,            
            "Price_D1": first_row.get("Price_D1", None),
            "Volume_D1": first_row.get("Volume_D1", None),
            "Short Float": first_row.get("Short Float", None),
            "Insider Own": first_row.get("Insider Own", None),
            "Inst Own": first_row.get("Inst Own", None),
            "Shs Float": first_row.get("Shs Float", None),
            "Shares Outstanding": first_row.get("Shares Outstanding", None),
            "Sector": first_row.get("Sector", None),
            "Industry": first_row.get("Industry", None),
            "Country": first_row.get("Country", None),
            "D1_Source": first_row.get("D1_Source", None),
        }
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

window_intervals = [
    ("5_15m",   5,  15),
    ("15_30m",  15, 30),
    ("30_45m",  30, 45),
    ("45_60m",  45, 60),
    ("60_90m",  60, 90),
    ("90_120m", 90, 120),
    ("120_240m",120, 240),
]

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

    # Filtra Pre-Market
    pm_df = dft[(dft["Datetime"] >= pm_start_dt) & (dft["Datetime"] <= pm_end_dt)].copy()
    pm_df = pm_df[~pm_df["Datetime"].dt.strftime("%H:%M").isin(["04:00", "04:01"])]

    # Filtra Regular Session
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

    # Correggi Open 09:30
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

    fund_data = fundamentals_dict.get(ticker, {})
    
    # Calcola D2_GAP% (open D2 vs close D1)
    price_d1 = fund_data.get("Price_D1")
    open_today = None
    
    if not rh_df.empty:
        open_today = rh_df.loc[rh_df["Datetime"] == rh_start_dt, "Open"].iloc[0] \
            if any(rh_df["Datetime"] == rh_start_dt) else rh_df["Open"].iloc[0]
    
    d2_gap_pct = None
    if price_d1 and open_today and price_d1 > 0:
        d2_gap_pct = ((open_today - price_d1) / price_d1) * 100

    # Costruisci riga base
    row = {
        "Ticker": ticker,
        "Date": max_date,
        "D1_Source": fund_data.get("D1_Source"),
        "Market Cap": fund_data.get("Market Cap"),
        "d1_change_from_open": fund_data.get("d1_change_from_open"),
        "d1_change": fund_data.get("d1_change"),
        "d1_gap": fund_data.get("d1_gap"),
        "Price_D1": price_d1,
        "Volume_D1": fund_data.get("Volume_D1"),
        "D2_GAP_%": round(d2_gap_pct, 2) if d2_gap_pct else None,
        "Short Float": fund_data.get("Short Float"),
        "Insider Own": fund_data.get("Insider Own"),
        "Inst Own": fund_data.get("Inst Own"),
        "Shs Float": fund_data.get("Shs Float"),
        "Shares Outstanding": fund_data.get("Shares Outstanding"),
        "Sector": fund_data.get("Sector"),
        "Industry": fund_data.get("Industry"),
        "Country": fund_data.get("Country"),
    }

    # --- VWAP ---
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
        
        rh_vol_df = rh_df[rh_df["Datetime"] > datetime.combine(max_date, time(9, 30))].copy()
        row["Volume"] = int(rh_vol_df["Volume"].sum()) if not rh_vol_df.empty else 0
    else:
        row.update({"Open": None, "High": None, "Low": None, "Close": None, "Volume": 0, "TimeHigh": None, "TimeLow": None})
        rh_vol_df = pd.DataFrame()

    # --- PRE-MARKET (con VolumePM corretto) ---
    if not pm_df.empty:
        # Cerca il volume alle 09:30 con Session = Pre-Market
        volpm_row = pd.DataFrame()
        
        if "Session" in dft.columns:
            volpm_row = dft[
                (dft["Datetime"].dt.time == time(9, 30)) & 
                (dft["Session"].astype(str).str.contains("Pre-Market", case=False, na=False))
            ]
        
        if volpm_row.empty:
            volpm_row = dft[dft["Datetime"].dt.time == time(9, 30)]
        
        if not volpm_row.empty:
            volpm = int(volpm_row["Volume"].iloc[0])
        else:
            volpm = int(pm_df["Volume"].iloc[-1]) if not pm_df.empty else 0

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
        
        if not rh_df.empty:
            target_dt = rh_start_dt + timedelta(minutes=m)
            close_row = rh_df.iloc[(rh_df["Datetime"] - target_dt).abs().argsort()[:1]]
            row[f"Close_{m}m"] = round(close_row["Close"].iloc[0], 2) if not close_row.empty else None
        else:
            row[f"Close_{m}m"] = None

    # --- High/Low per finestre temporali ---
    if not rh_df.empty:
        for label, start_m, end_m in window_intervals:
            start_dt = rh_start_dt + timedelta(minutes=start_m)
            end_dt   = rh_start_dt + timedelta(minutes=end_m)
            window_df = rh_df[(rh_df["Datetime"] > start_dt) & (rh_df["Datetime"] <= end_dt)]
            row[f"High_{label}"] = round(window_df["High"].max(), 2) if not window_df.empty else None
            row[f"Low_{label}"]  = round(window_df["Low"].min(),  2) if not window_df.empty else None
    else:
        for label, _, _ in window_intervals:
            row[f"High_{label}"] = None
            row[f"Low_{label}"]  = None

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

window_cols = []
for label, _, _ in window_intervals:
    window_cols.extend([f"High_{label}", f"Low_{label}"])
    
cols_fixed = [
    "Ticker", "Date",
    "D1_Source", "Sector", "Industry", "Country",
    "Market Cap",
    "d1_change_from_open", "d1_change", "d1_gap",
    "Price_D1", "Volume_D1", "D2_GAP_%",
    "Short Float", "Insider Own", "Inst Own", "Shs Float", "Shares Outstanding",
    "VWAP_0930",
    "Open", "High", "Low", "Close", "Volume",
    "TimeHigh", "TimeLow",
    "OpenPM", "HighPM", "LowPM", "ClosePM", "VolumePM", "TimePMH"
]

final_columns = [c for c in cols_fixed if c in df_final.columns]
final_columns += [c for c in intraday_blocks if c in df_final.columns]
final_columns += [c for c in window_cols if c in df_final.columns]

df_final = df_final[final_columns]

# Salva
df_final.to_excel(output_path, index=False)
print(f"\n✅ File riepilogativo salvato: {output_path}")
print(f"📊 Totale ticker: {len(df_final)}")