import requests
import re
import os
import pandas as pd
from datetime import datetime
import time
import random

CITIES = [
    {"name": "SAEZ", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/ezeiza_argentina_3435038"},
    {"name": "SBGR", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/guarulhos_brazil_3461786"},
    {"name": "KMIA", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/miami_united-states_4164138"},
    {"name": "KATL", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/atlanta_united-states_4180439"},
    {"name": "KLGA", "url": "https://www.meteoblue.com/en/weather/forecast/multimodel/la-guardia-airport_united-states_5123968"}
]

def scrape_forecast(city_name, url):
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    try:
        # Espera aleatoria para no saturar
        time.sleep(random.uniform(3, 7))
        r = requests.get(url, headers=headers, timeout=30)
        html = r.text
        
        # BUSQUEDA FLEXIBLE: Acepta espacios, comillas simples o dobles
        # Buscamos patrones como: data-time="20240308 2100" y data-temp="22"
        times = re.findall(r'data-time=["\']([^"\']+)["\']', html)
        temps = re.findall(r'data-temp=["\']([^"\']+)["\']', html)
        
        if not times or not temps:
            print(f"⚠️ Sin datos en {city_name}. Probando patrón alternativo...")
            # Intento alternativo por si los datos están en formato JSON dentro del HTML
            times = re.findall(r'\"time\"\:\"(\d{8}\s\d{4})\"', html)
            temps = re.findall(r'\"temp\"\:\"([\d\.\-]+)\"', html)

        if not times or not temps:
            print(f"❌ Fallo total en {city_name}. Tamaño: {len(html)}")
            return None

        ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        data = []

        # Aseguramos que tengan la misma longitud para no desfasar datos
        min_len = min(len(times), len(temps))
        
        for i in range(min_len):
            t = times[i]
            temp = temps[i]
            
            try:
                # Limpiamos el string de tiempo (debe ser YYYYMMDD HHMM)
                clean_t = re.sub(r'[^0-9 ]', '', t)
                f_date = f"{clean_t[:4]}-{clean_t[4:6]}-{clean_t[6:8]}"
                f_hour = f"{clean_t[9:11]}:{clean_t[11:13]}"
                
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
        print(f"Error en {city_name}: {e}")
        return None

def main():
    base_path = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_path, "forecasts")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for city in CITIES:
        print(f"Analizando {city['name']}...")
        df = scrape_forecast(city['name'], city['url'])
        if df is not None and not df.empty:
            filename = os.path.join(output_dir, f"forecast_{city['name'].lower()}.csv")
            if not os.path.exists(filename):
                df.to_csv(filename, index=False)
            else:
                df.to_csv(filename, mode='a', header=False, index=False)
            print(f"✅ {city['name']}: {len(df)} filas nuevas.")
        else:
            print(f"⏭️ {city['name']} saltado (sin datos).")

if __name__ == "__main__":
    main()
    
