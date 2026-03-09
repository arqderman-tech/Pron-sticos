import requests
import pandas as pd
import os
from datetime import datetime

# Tus aeropuertos originales
AIRPORTS = {
    "SAEZ": {"lat": -34.82, "lon": -58.53, "usa": False},
    "SBGR": {"lat": -23.43, "lon": -46.47, "usa": False},
    "KMIA": {"lat": 25.79, "lon": -80.29, "usa": True},
    "KATL": {"lat": 33.64, "lon": -84.42, "usa": True},
    "KLGA": {"lat": 40.77, "lon": -73.87, "usa": True}
}

def get_single_model(lat, lon, model):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m",
        "models": model,
        "timezone": "UTC", "forecast_days": 3
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()["hourly"]
            # El nombre de la clave de temperatura varía según el modelo
            temp_key = [k for k in data.keys() if "temperature_2m" in k][0]
            return pd.DataFrame({
                "pronostico_para": [t.replace("T", " ") for t in data["time"]],
                f"temp_{model.split('_')[0]}": data[temp_key]
            })
    except:
        pass
    return None

def main():
    os.makedirs("forecasts", exist_ok=True)
    ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    
    for icao, info in AIRPORTS.items():
        print(f"Procesando {icao}...")
        
        # Lista de modelos a intentar para este aeropuerto
        modelos_test = ["ecmwf_ifs04", "gfs_seamless", "icon_seamless", "gem_seamless"]
        if info["usa"]:
            modelos_test.append("hrrr_seamless")
        
        df_base = None
        
        for m in modelos_test:
            df_model = get_single_model(info["lat"], info["lon"], m)
            if df_model is not None:
                if df_base is None:
                    df_base = df_model
                else:
                    df_base = pd.merge(df_base, df_model, on="pronostico_para", how="outer")
        
        if df_base is not None:
            df_base["descarga_utc"] = ahora_utc
            # Reordenar columnas
            cols = ["descarga_utc", "pronostico_para"] + [c for c in df_base.columns if "temp_" in c]
            df_base = df_base[cols]
            
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            file_exists = os.path.isfile(filename)
            df_base.to_csv(filename, mode='a', index=False, header=not file_exists)
            print(f"✅ {icao} guardado con {len(df_base.columns)-2} modelos.")

if __name__ == "__main__":
    main()
    
