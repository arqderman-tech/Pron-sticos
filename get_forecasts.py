import requests
import pandas as pd
import os
from datetime import datetime

# Coordenadas exactas de tus aeropuertos
AEROPUERTOS = {
    "SAEZ": {"lat": -34.82, "lon": -58.53},
    "SBGR": {"lat": -23.43, "lon": -46.47},
    "KMIA": {"lat": 25.79, "lon": -80.29},
    "KATL": {"lat": 33.64, "lon": -84.42},
    "KLGA": {"lat": 40.77, "lon": -73.87}
}

def get_forecast_api(icao, lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": "ecmwf_ifs04,gfs_seamless,icon_seamless", # 3 modelos top
        "timezone": "UTC",
        "forecast_days": 3
    }
    
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        res = r.json()
        
        hourly = res["hourly"]
        df = pd.DataFrame({
            "momento_descarga_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            "pronostico_para": hourly["time"],
            "temp_ecmwf": hourly.get("temperature_2m_ecmwf_ifs04"),
            "temp_gfs": hourly.get("temperature_2m_gfs_seamless"),
            "temp_icon": hourly.get("temperature_2m_icon_seamless")
        })
        
        # Limpiamos el formato de fecha de la API (T -> espacio)
        df["pronostico_para"] = df["pronostico_para"].str.replace("T", " ")
        return df
        
    except Exception as e:
        print(f"Error en {icao}: {e}")
        return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    for icao, coord in AEROPUERTOS.items():
        print(f"Obteniendo API para {icao}...")
        df = get_forecast_api(icao, coord["lat"], coord["lon"])
        
        if df is not None:
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            if not os.path.exists(filename):
                df.to_csv(filename, index=False)
            else:
                df.to_csv(filename, mode='a', header=False, index=False)
            print(f"✅ {icao} actualizado con 3 modelos.")

if __name__ == "__main__":
    main()
