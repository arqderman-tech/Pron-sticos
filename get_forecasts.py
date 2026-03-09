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
    # 'best_match' selecciona automáticamente los mejores modelos (GFS, ECMWF, ICON, etc.)
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": "best_match", 
        "timezone": "UTC",
        "forecast_days": 3
    }
    
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            res = r.json()
            hourly = res["hourly"]
            
            df = pd.DataFrame({"pronostico_para": [t.replace("T", " ") for t in hourly["time"]]})
            
            for key, values in hourly.items():
                if key != "time":
                    # Limpiamos el nombre: temperature_2m_ecmwf_ifs04 -> temp_ecmwf
                    parts = key.replace("temperature_2m_", "temp_").split("_")
                    col_name = f"{parts[0]}_{parts[1]}"
                    df[col_name] = values
            
            df["descarga_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            return df
        else:
            print(f"❌ Error en {icao}: {r.text}")
    except Exception as e:
        print(f"❌ Fallo en {icao}: {e}")
    return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    for icao, coord in AEROPUERTOS.items():
        print(f"Buscando modelos para {icao}...")
        df = get_forecast(icao, coord["lat"], coord["lon"])
        
        if df is not None:
            # Ordenar columnas: descarga, tiempo, y luego los modelos
            cols = ["descarga_utc", "pronostico_para"] + [c for c in df.columns if "temp_" in c]
            df = df[cols]
            
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            file_exists = os.path.isfile(filename)
            df.to_csv(filename, mode='a', index=False, header=not file_exists)
            print(f"✅ {icao} guardado. Modelos detectados: {len(df.columns)-2}")

if __name__ == "__main__":
    main()
    
