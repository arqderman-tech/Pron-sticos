import requests
import re
import os
import pandas as pd
from datetime import datetime

# --- CONFIGURACIÓN DE CIUDADES (URLs MultiModel) ---
CITIES = [
    {"name": "SAEZ", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/ezeiza_argentina_3435038"},
    {"name": "SBGR", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/guarulhos_brazil_3461786"},
    {"name": "KMIA", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/miami_united-states_4164138"},
    {"name": "KATL", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/atlanta_united-states_4180439"},
    {"name": "KLGA", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/la-guardia-airport_united-states_5123968"}
]

def scrape_forecast(city_name, url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        html = r.text
        
        # Extraer tiempos y temperaturas usando la lógica de tu metereo2.py
        times = re.findall(r'data-time="([^"]+)"', html)
        temps = re.findall(r'data-temp="([^"]+)"', html)
        
        if not times or not temps:
            return None

        ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        data = []

        for t, temp in zip(times, temps):
            # Parsear el tiempo del pronóstico (YYYYMMDD HHMM)
            f_date = f"{t[:4]}-{t[4:6]}-{t[6:8]}"
            f_hour = f"{t[9:11]}:{t[11:13]}"
            
            # Calcular lead_time_h (antelación)
            f_dt = datetime.strptime(f"{f_date} {f_hour}", "%Y-%m-%d %H:%M")
            c_dt = datetime.strptime(ahora_utc, "%Y-%m-%d %H:%M")
            lead_h = int((f_dt - c_dt).total_seconds() // 3600)

            data.append({
                "descarga_utc": ahora_utc,
                "pronostico_dia": f_date,
                "pronostico_hora": f_hour,
                "temp_c": temp,
                "lead_time_h": lead_h
            })
            
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error en {city_name}: {e}")
        return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    for city in CITIES:
        print(f"Procesando {city['name']}...")
        df = scrape_forecast(city['name'], city['url'])
        if df is not None:
            filename = f"forecasts/forecast_{city['name'].lower()}.csv"
            # Append sin repetir cabecera
            if not os.path.exists(filename):
                df.to_csv(filename, index=False)
            else:
                df.to_csv(filename, mode='a', header=False, index=False)
            print(f"✅ Guardados {len(df)} puntos para {city['name']}")

if __name__ == "__main__":
    main()
