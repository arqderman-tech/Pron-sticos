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

# Solo los 10 mejores modelos globales (Garantizados 100% de disponibilidad)
MODELOS = [
    "ecmwf_ifs04", "gfs_seamless", "icon_seamless", "gem_seamless",
    "meteofrance_seamless", "ukmo_seamless", "jma_seamless", 
    "bom_access", "cma_grapes_global", "arpege_world"
]

def get_forecast(icao, lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m",
        "models": ",".join(MODELOS),
        "timezone": "UTC", "forecast_days": 3
    }
    
    try:
        # Timeout corto: si no responde en 15 seg, algo anda mal
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            res = r.json()
            hourly = res["hourly"]
            
            # Crear DataFrame base
            df = pd.DataFrame({"pronostico_para": [t.replace("T", " ") for t in hourly["time"]]})
            
            # Mapear temperaturas
            for key, values in hourly.items():
                if key != "time":
                    # Limpiar nombre: temperature_2m_ecmwf_ifs04 -> temp_ecmwf
                    col_name = key.replace("temperature_2m_", "temp_").split("_")[0:2]
                    df["_".join(col_name)] = values
            
            df["descarga_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            return df
        else:
            print(f"❌ Error {r.status_code} en {icao}: {r.text[:100]}")
    except Exception as e:
        print(f"❌ Fallo en {icao}: {e}")
    return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    for icao, coord in AEROPUERTOS.items():
        print(f"Solicitando datos para {icao}...")
        df = get_forecast(icao, coord["lat"], coord["lon"])
        
        if df is not None:
            # Reordenar: descarga primero
            cols = ["descarga_utc", "pronostico_para"] + [c for c in df.columns if "temp_" in c]
            df = df[cols]
            
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            file_exists = os.path.isfile(filename)
            df.to_csv(filename, mode='a', index=False, header=not file_exists)
            print(f"✅ {icao} guardado exitosamente.")

if __name__ == "__main__":
    main()
    
