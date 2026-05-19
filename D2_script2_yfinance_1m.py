# D2_gainers_intraday.py
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
print(f"📂 Leggo file gainers di ieri: {yesterday_str}")
print(f"🎯 Recupero dati intraday per: {intraday_date_str}")

# ------------------------
# 📥 Legge ticker dal file gainers di ieri
# ------------------------
input_dir = "output"
ticker_file = os.path.join(input_dir, f"gainers_{yesterday_str}.csv")

if not os.path.exists(ticker_file):
    print(f"❌ File non trovato: {ticker_file}")
    exit(1)

# 🆕 FUNZIONE DI CONVERSIONE
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

df_tickers = pd.read_csv(ticker_file)

# 🆕 CONVERSIONE colonne con suffisso
for col in ['Shs Float', 'Shares Outstanding', 'Market Cap', 'Volume']:
    if col in df_tickers.columns:
        df_tickers[col] = df_tickers[col].apply(convert_finviz_number)

tickers = df_tickers['Ticker'].dropna().unique().tolist()
finviz_map = df_tickers.set_index('Ticker').to_dict('index')

print(f"📊 Trovati {len(tickers)} ticker")


# ------------------------
# Lista per risultati finali
# ------------------------
final_rows = []

for ticker in tickers:
    print(f"\n📈 Analizzo: {ticker}")
    stock = yf.Ticker(ticker)

    # ------------------------
    # Dati fondamentali - INCLUSO SECTOR, INDUSTRY, COUNTRY
    # ------------------------
    fundamentals = {
        "Ticker": ticker,
        "Market Cap": finviz_map.get(ticker, {}).get("Market Cap"),
        "Gain_%": finviz_map.get(ticker, {}).get("Gain_%"),
        "Price_Gain_Giorno": finviz_map.get(ticker, {}).get("Price"),
        "Volume_Gain_Giorno": finviz_map.get(ticker, {}).get("Volume"),
        "Shs Float": finviz_map.get(ticker, {}).get("Shs Float"),
        "Insider Own": finviz_map.get(ticker, {}).get("Insider Own"),
        "Inst Own": finviz_map.get(ticker, {}).get("Inst Own"),
        "Short Float": finviz_map.get(ticker, {}).get("Short Float"),
        "Shares Outstanding": finviz_map.get(ticker, {}).get("Shares Outstanding"),
        # 🆕 NUOVE COLONNE
        "Sector": finviz_map.get(ticker, {}).get("Sector"),
        "Industry": finviz_map.get(ticker, {}).get("Industry"),
        "Country": finviz_map.get(ticker, {}).get("Country")
    }
    
    # ------------------------
    # Dati intraday (stessa logica di prima)
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

        print(f"✅ {ticker} - {len(pre_market)} PM, {len(regular_market)} REG")

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