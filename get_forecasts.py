import requests
import pandas as pd
import os
from datetime import datetime

AEROPUERTOS = {
    "SAEZ": {"lat": -34.82, "lon": -58.53},
    "SBGR": {"lat": -23.43, "lon": -46.47},
    "KMIA": {"lat": 25.79, "lon": -80.29},
    "KATL": {"lat": 33.64, "lon": -84.42},
    "KLGA": {"lat": 40.77, "lon": -73.87}
}

def get_forecast(icao, lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    # Lista de modelos globales validados que no causan el error MultiDomains
    modelos = [
        "ecmwf_ifs04",    # Europeo (Líder mundial)
        "gfs_seamless",   # Americano (Referencia)
        "icon_seamless",  # Alemán (Muy preciso en el sur)
        "gem_seamless",   # Canadiense
        "ukmo_seamless",  # Reino Unido
        "meteofrance_seamless", # Francés
        "jma_seamless",   # Japonés
        "bom_access"      # Australiano (Clave para hemisferio sur)
    ]
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": ",".join(modelos),
        "timezone": "UTC",
        "forecast_days": 3
    }
    
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            res = r.json()
            hourly = res["hourly"]
            
            # DataFrame base con los tiempos
            df = pd.DataFrame({"pronostico_para": [t.replace("T", " ") for t in hourly["time"]]})
            
            # Mapeo dinámico: busca cualquier columna que sea de temperatura
            for key, values in hourly.items():
                if "temperature_2m" in key:
                    # Formato: temp_ecmwf, temp_gfs, etc.
                    nombre_col = key.replace("temperature_2m_", "temp_").split("_")[0:2]
                    df["_".join(nombre_col)] = values
            
            df["descarga_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            return df
        else:
            # Si falla, imprimimos el error exacto para debuguear en el log de GitHub
            print(f"⚠️ Error {r.status_code} en {icao}: {r.text}")
    except Exception as e:
        print(f"❌ Fallo crítico en {icao}: {e}")
    return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    for icao, coord in AEROPUERTOS.items():
        print(f"Extrayendo ensamble para {icao}...")
        df = get_forecast(icao, coord["lat"], coord["lon"])
        
        if df is not None:
            # Reordenar columnas para análisis rápido
            cols = ["descarga_utc", "pronostico_para"] + [c for c in df.columns if "temp_" in c]
            df = df[cols]
            
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            file_exists = os.path.isfile(filename)
            # Guardado incremental sin pisar datos viejos
            df.to_csv(filename, mode='a', index=False, header=not file_exists)
            print(f"✅ {icao} actualizado. Modelos: {len(df.columns)-2}")

if __name__ == "__main__":
    main()
    
