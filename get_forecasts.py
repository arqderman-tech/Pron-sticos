import requests
import pandas as pd
import os
from datetime import datetime
import time

AEROPUERTOS = {
    "SAEZ": {"lat": -34.82, "lon": -58.53},
    "SBGR": {"lat": -23.43, "lon": -46.47},
    "KMIA": {"lat": 25.79, "lon": -80.29},
    "KATL": {"lat": 33.64, "lon": -84.42},
    "KLGA": {"lat": 40.77, "lon": -73.87}
}

# Lista exhaustiva de modelos disponibles en Open-Meteo
MODELOS_A_PROBAR = [
    "ecmwf_ifs04", "gfs_seamless", "icon_seamless", "gem_seamless",
    "meteofrance_seamless", "ukmo_seamless", "jma_seamless", "bom_access",
    "cma_grapes_global", "knmi_seamless", "metno_nordic", "dwd_icon_eu",
    "jma_msm", "gfs_global", "arpege_world", "arpege_europe"
]

def get_data_for_model(icao, lat, lon, model):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m",
        "models": model,
        "timezone": "UTC", "forecast_days": 3
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            res = r.json()
            # Extraer tiempos y temperaturas
            times = res["hourly"]["time"]
            # El nombre de la temperatura en el JSON incluye el nombre del modelo
            temp_key = [k for k in res["hourly"].keys() if "temperature_2m" in k][0]
            temps = res["hourly"][temp_key]
            
            return pd.DataFrame({
                "pronostico_para": [t.replace("T", " ") for t in times],
                f"temp_{model}": temps
            })
    except:
        pass
    return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

    for icao, coord in AEROPUERTOS.items():
        print(f"--- Procesando {icao} ---")
        df_final = None
        
        for model in MODELOS_A_PROBAR:
            # Pedimos cada modelo individualmente
            df_model = get_data_for_model(icao, coord["lat"], coord["lon"], model)
            
            if df_model is not None:
                if df_final is None:
                    df_final = df_model
                else:
                    # Unimos por la columna del tiempo
                    df_final = pd.merge(df_final, df_model, on="pronostico_para", how="outer")
                print(f"  ✅ {model}: capturado")
            else:
                print(f"  ❌ {model}: no disponible o falló")
            
            time.sleep(0.2) # Evitar ban por flood

        if df_final is not None:
            # Agregamos la columna de cuándo se bajó
            df_final["descarga_utc"] = ahora_utc
            
            # Reordenamos para que descarga_utc esté al principio
            cols = ["descarga_utc", "pronostico_para"] + [c for c in df_final.columns if c not in ["descarga_utc", "pronostico_para"]]
            df_final = df_final[cols]
            
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            file_exists = os.path.isfile(filename)
            df_final.to_csv(filename, mode='a', index=False, header=not file_exists)
            print(f"💾 {icao}: Guardado con {len(df_final.columns) - 2} modelos exitosos.\n")

if __name__ == "__main__":
    main()
