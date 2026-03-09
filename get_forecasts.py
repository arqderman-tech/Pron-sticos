import requests
import pandas as pd
import os
from datetime import datetime

# Definimos los aeropuertos
AEROPUERTOS = {
    "SAEZ": {"lat": -34.82, "lon": -58.53},
    "SBGR": {"lat": -23.43, "lon": -46.47},
    "KMIA": {"lat": 25.79, "lon": -80.29},
    "KATL": {"lat": 33.64, "lon": -84.42},
    "KLGA": {"lat": 40.77, "lon": -73.87}
}

def get_forecast_api(icao, lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    
    # 10 Modelos Globales y Regionales para máxima dispersión
    models_list = [
        "ecmwf_ifs04", "gfs_seamless", "icon_seamless", 
        "gem_seamless", "meteofrance_seamless", "ukmo_seamless", 
        "jma_seamless", "bom_access", "metno_nordic", "cma_grapes_global"
    ]
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": ",".join(models_list),
        "timezone": "UTC",
        "forecast_days": 3 # Guardamos los próximos 3 días hora por hora
    }
    
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        res = r.json()
        
        hourly = res["hourly"]
        ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        
        # Estructura: Una fila por cada hora del pronóstico
        data_dict = {
            "descarga_utc": ahora_utc, # Cuándo se bajó el dato
            "pronostico_para": [t.replace("T", " ") for t in hourly["time"]] # Para qué hora es
        }
        
        for key in hourly.keys():
            if key == "time": continue
            # Limpieza de nombres: temperature_2m_ecmwf_ifs04 -> temp_ecmwf
            nombre_col = key.replace("temperature_2m_", "temp_").split("_")[0:2]
            data_dict["_".join(nombre_col)] = hourly[key]
        
        return pd.DataFrame(data_dict)
        
    except Exception as e:
        print(f"Error en {icao}: {e}")
        return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    for icao, coord in AEROPUERTOS.items():
        print(f"Procesando {icao}...")
        df = get_forecast_api(icao, coord["lat"], coord["lon"])
        
        if df is not None:
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            
            # Verificamos si el archivo existe para saber si poner encabezado
            file_exists = os.path.isfile(filename)
            
            # Guardado incremental: NO pisa lo anterior, agrega al final
            df.to_csv(filename, mode='a', index=False, header=not file_exists)
            
            print(f"✅ {icao}: Guardadas {len(df)} filas (predicciones hora por hora).")

if __name__ == "__main__":
    main()
    
