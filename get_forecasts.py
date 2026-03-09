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
    # Lista de User-Agents para rotar y parecer humanos diferentes
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ]

    session = requests.Session()
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Alt-Used": "www.meteoblue.com",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

    try:
        # Primero visitamos la home o un delay para no ser tan agresivos
        time.sleep(random.uniform(2, 5))
        r = session.get(url, headers=headers, timeout=30)
        html = r.text
        
        # El regex ahora busca patrones más flexibles por si el HTML varía espacios
        times = re.findall(r'data-time\s*=\s*"([^"]+)"', html)
        temps = re.findall(r'data-temp\s*=\s*"([^"]+)"', html)
        
        if not times or not temps:
            # Si falla, imprimimos un pedacito del HTML para debuguear en el log de GitHub
            print(f"⚠️ No se hallaron datos para {city_name}. Tamaño HTML: {len(html)}")
            if "forbidden" in html.lower() or "captcha" in html.lower():
                print("🚫 Bloqueo detectado (403 o Captcha)")
            return None

        ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        data = []

        for t, temp in zip(times, temps):
            f_date = f"{t[:4]}-{t[4:6]}-{t[6:8]}"
            f_hour = f"{t[9:11]}:{t[11:13]}"
            
            try:
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
            except: continue
            
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
        print(f"Procesando {city['name']}...")
        df = scrape_forecast(city['name'], city['url'])
        if df is not None:
            filename = os.path.join(output_dir, f"forecast_{city['name'].lower()}.csv")
            if not os.path.exists(filename):
                df.to_csv(filename, index=False)
            else:
                df.to_csv(filename, mode='a', header=False, index=False)
            print(f"✅ {city['name']}: {len(df)} puntos guardados.")

if __name__ == "__main__":
    main()
    
