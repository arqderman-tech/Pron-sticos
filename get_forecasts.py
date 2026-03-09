import requests
import re
import os
import pandas as pd
from datetime import datetime
import time

CITIES = [
    {"name": "SAEZ", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/ezeiza_argentina_3435038"},
    {"name": "SBGR", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/guarulhos_brazil_3461786"},
    {"name": "KMIA", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/miami_united-states_4164138"},
    {"name": "KATL", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/atlanta_united-states_4180439"},
    {"name": "KLGA", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/la-guardia-airport_united-states_5123968"}
]

def scrape_forecast(city_name, url):
    # Usamos una sesión para mantener cookies, esto ayuda a pasar el bloqueo
    session = requests.Session()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.meteoblue.com/en/weather/forecast/multimodel",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    try:
        # 1. Primero pedimos la URL para obtener cookies de sesión
        response = session.get(url, headers=headers, timeout=30)
        html = response.text
        
        # 2. Tu Regex original de meteo2.py
        times = re.findall(r'data-time="([^"]+)"', html)
        temps = re.findall(r'data-temp="([^"]+)"', html)
        
        if not times or not temps:
            print(f"⚠️ Error en {city_name}: No se detectaron las etiquetas data-time/data-temp.")
            return None

        ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        data = []

        # 3. Procesamiento y armado de filas
        for t, temp in zip(times, temps):
            # Formato: YYYYMMDD HHMM
            f_date = f"{t[:4]}-{t[4:6]}-{t[6:8]}"
            f_hour = f"{t[9:11]}:{t[11:13]}"
            
            # Calculamos lead_time (cuántas horas antes se hizo la predicción)
            try:
                f_dt = datetime.strptime(f"{f_date} {f_hour}", "%Y-%m-%d %H:%M")
                c_dt = datetime.strptime(ahora_utc, "%Y-%m-%d %H:%M")
                lead_h = int((f_dt - c_dt).total_seconds() // 3600)
                
                data.append({
                    "momento_descarga_utc": ahora_utc,
                    "pronostico_dia": f_date,
                    "pronostico_hora": f_hour,
                    "temp_c": temp,
                    "lead_time_h": lead_h
                })
            except:
                continue
            
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error de conexión en {city_name}: {e}")
        return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    
    for city in CITIES:
        print(f"Capturando {city['name']}...")
        df = scrape_forecast(city['name'], city['url'])
        
        if df is not None and not df.empty:
            filename = f"forecasts/forecast_{city['name'].lower()}.csv"
            # Si no existe, con cabecera; si existe, append sin cabecera.
            if not os.path.exists(filename):
                df.to_csv(filename, index=False)
            else:
                df.to_csv(filename, mode='a', header=False, index=False)
            print(f"✅ {city['name']}: {len(df)} filas nuevas.")
        
        # Pausa breve entre ciudades para no alertar al firewall
        time.sleep(5)

if __name__ == "__main__":
    main()
