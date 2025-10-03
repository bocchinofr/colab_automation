import os
import pandas as pd
from datetime import datetime, time

# --- Configurazioni ---
input_folder = "output"
output_folder = "output/aggregati"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# --- Trova l'ultimo file intraday generato ---
files = [f for f in os.listdir(input_folder) if f.startswith("intraday1m_all_") and f.endswith(".csv")]
if not files:
    raise FileNotFoundError("Nessun file intraday trovato in 'output/'")

files.sort()
input_file = os.path.join(input_folder, files[-1])
print(f"📂 Elaboro file: {input_file}")

# --- Leggi CSV ---
df = pd.read_csv(input_file, parse_dates=['date'])
df.columns = [c.strip() for c in df.columns]
df['time_only'] = df['date'].dt.time

# --- Funzione generica per aggregare intervalli ---
def aggregate_interval(df, start_time, end_time, interval_label, include_open_close=True, include_time=False):
    df_int = df[(df['time_only'] >= start_time) & (df['time_only'] <= end_time)].copy()
    df_int['Date'] = df_int['date'].dt.date  # <-- assicura di avere la colonna Date
    df_int = df_int.sort_values(['Ticker', 'date'])
    
    if df_int.empty:
        return pd.DataFrame()
    
    agg_dict = {
        'High': 'max',
        'Low': 'min',
        'Volume': 'sum'
    }
    if include_open_close:
        agg_dict.update({'Open': 'first', 'Close': 'last'})
    
    # Raggruppa per ticker e data
    agg_df = df_int.groupby(['Ticker','Date']).agg(agg_dict).reset_index()
    
    # calcolo orario del max/min se richiesto
    if include_time:
        high_time_list, low_time_list = [], []
        for (ticker,date_), group in df_int.groupby(['Ticker','Date']):
            idx_high = group['High'].idxmax()
            idx_low = group['Low'].idxmin()
            high_time_list.append((ticker, date_, group.loc[idx_high, 'date'].time()))
            low_time_list.append((ticker, date_, group.loc[idx_low, 'date'].time()))
        if include_open_close and interval_label.startswith("PM"):
            agg_df['HighPMTime'] = pd.DataFrame(high_time_list, columns=['Ticker','Date','HighPMTime'])['HighPMTime']
        else:
            agg_df['highREGTime'] = pd.DataFrame(high_time_list, columns=['Ticker','Date','highREGTime'])['highREGTime']
            agg_df['lowREGTime'] = pd.DataFrame(low_time_list, columns=['Ticker','Date','lowREGTime'])['lowREGTime']
    
    agg_df.rename(columns={
        'Open': f'open{interval_label}' if include_open_close else None,
        'Close': f'close{interval_label}' if include_open_close else None,
        'High': f'high{interval_label}',
        'Low': f'low{interval_label}',
        'Volume': f'volume{interval_label}'
    }, inplace=True)
    
    agg_df = agg_df[[c for c in agg_df.columns if c is not None]]
    
    return agg_df

# --- Intervalli da aggregare ---
intervals = [
    # Pre-market
    (time(4,0), time(9,29), "PM", True, True),
    # Pre-market 15m
    (time(4,0), time(4,15), "PM15m", False, False),
    # Regolare vari intervalli
    (time(9,30), time(9,31), "1m", False, False),
    (time(9,30), time(9,35), "5m", False, False),
    (time(9,30), time(10,0), "30m", False, False),
    (time(9,30), time(10,30), "1h", False, False),
    (time(9,30), time(11,0), "1_30h", False, False),
    (time(9,30), time(13,30), "4h", False, False),
    # Sessione regolare completa
    (time(9,30), time(15,59), "REG", True, True)
]

agg_dfs = []
for start, end, label, include_oc, include_time in intervals:
    df_agg = aggregate_interval(df, start, end, label, include_open_close=include_oc, include_time=include_time)
    if not df_agg.empty:
        agg_dfs.append(df_agg)

# --- Merge di tutti gli intervalli su 'Ticker' ---
from functools import reduce
df_final = reduce(lambda left, right: pd.merge(left, right, on=['Ticker','Date'], how='outer'), agg_dfs)

# --- Ordine colonne finale ---
column_order = [
    'Ticker', 'Date', 'openREG','highREG','lowREG','closeREG','volumeREG','highREGTime','lowREGTime',
    'openPM','highPM','lowPM','closePM','volumePM','HighPMTime','highPM15m','lowPM15m',
    'high1m','low1m','volume1m','high5m','low5m','volume5m','high30m','low30m','volume30m',
    'high1h','low1h','volume1h','high1_30h','low1_30h','volume1_30h','high4h','low4h','volume4h'
]

# --- Assicura tutte le colonne richieste siano presenti ---
for col in column_order:
    if col not in df_final.columns:
        df_final[col] = pd.NA

df_final = df_final[column_order]

# --- Salva CSV finale ---
today_str = datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(output_folder, f"aggregato_colonna_{today_str}.csv")
df_final.to_csv(output_file, index=False, float_format="%.4f")

print(f"\n✅ File finale in colonna creato: {output_file}")
print(df_final.head())
