import os
from datetime import datetime, timedelta

# 🔧 Parametri da modificare
start_date = "2024-12-01"
end_date = "2024-12-31"
output_folder = "colab_automation/output"  # Percorso relativo o assoluto

# ✅ Converte le date in oggetti datetime
start_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_dt = datetime.strptime(end_date, "%Y-%m-%d")

# 📦 Lista dei file nella cartella output
try:
    all_files = os.listdir(output_folder)
except FileNotFoundError:
    print(f"❌ Cartella non trovata: {output_folder}")
    all_files = []

deleted_files = []

# 🔁 Ciclo su ogni data nell'intervallo
current_dt = start_dt
while current_dt <= end_dt:
    date_str = current_dt.strftime("%Y-%m-%d")
    patterns = [
        f"dati_azioni_completo_{date_str}.xlsx",
        f"tickers_{date_str}.csv"
    ]

    for pattern in patterns:
        file_path = os.path.join(output_folder, pattern)
        if os.path.exists(file_path):
            os.remove(file_path)
            deleted_files.append(pattern)
            print(f"✅ Eliminato: {pattern}")
        else:
            print(f"⚠️ Non trovato: {pattern}")

    current_dt += timedelta(days=1)

# 🧾 Report finale
print("\n🧹 Pulizia completata.")
print(f"Totale file eliminati: {len(deleted_files)}")
