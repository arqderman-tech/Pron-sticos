import requests
import pandas as pd
import os
from datetime import datetime

# Tus aeropuertos
AIRPORTS = {
    "SAEZ": {"lat": -34.82, "lon": -58.53, "usa": False},
    "SBGR": {"lat": -23.43, "lon": -46.47, "usa": False},
    "KMIA": {"lat": 25.79,  "lon": -80.29,  "usa": True},
    "KATL": {"lat": 33.64,  "lon": -84.42,  "usa": True},
    "KLGA": {"lat": 40.77,  "lon": -73.87,  "usa": True}
}

def get_forecast(lat, lon, models_list="auto"):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": models_list if isinstance(models_list, str) else ",".join(models_list),
        "timezone": "UTC",
        "forecast_days": 3
    }
    
    try:
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        data = r.json()
        
        if "hourly" not in data or "time" not in data["hourly"]:
            print("  Respuesta sin datos horarios")
            return None
            
        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature_2m": data["hourly"]["temperature_2m"],
        })
        return df
        
    except Exception as e:
        print(f"  Error en API: {e}")
        return None


def main():
    os.makedirs("forecasts", exist_ok=True)
    ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    for icao, info in AIRPORTS.items():
        print(f"Procesando {icao} ({info['lat']}, {info['lon']})...")
        
        modelos = ["auto"]  # o ["gfs", "icon", "ecmwf"] si querés separar
        
        df = None
        
        for m in modelos:
            print(f"  → {m}")
            df_model = get_forecast(info["lat"], info["lon"], m)
            if df_model is not None:
                df_model = df_model.rename(columns={"temperature_2m": f"temp_{m}"})
                if df is None:
                    df = df_model
                else:
                    df = pd.merge(df, df_model, on="time", how="outer")
        
        if df is not None:
            df["descarga_utc"] = ahora_utc
            df["aeropuerto"] = icao
            
            cols = ["descarga_utc", "aeropuerto", "time"] + \
                   [c for c in df.columns if c.startswith("temp_")]
            df = df[cols]
            
            df["time"] = pd.to_datetime(df["time"])
            
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            
            if os.path.isfile(filename):
                try:
                    old = pd.read_csv(filename)
                    old["time"] = pd.to_datetime(old["time"])
                    # ← Línea corregida: sin backslash antes del \~
                    df_new = df[\~df["time"].isin(old["time"])]
                    if not df_new.empty:
                        df_new.to_csv(filename, mode='a', header=False, index=False)
                        print(f"  → Agregadas {len(df_new)} filas nuevas")
                    else:
                        print("  → No hay datos nuevos")
                except Exception as e:
                    print(f"  Error leyendo archivo viejo: {e}")
                    # Si falla, sobrescribe para no perder el run
                    df.to_csv(filename, index=False)
                    print("  → Archivo sobrescrito por error")
            else:
                df.to_csv(filename, index=False)
                print(f"  → Archivo creado con {len(df)} filas")
            
            print(f"✅ {icao} OK")
        else:
            print(f"❌ {icao} sin datos")


if __name__ == "__main__":
    main()
