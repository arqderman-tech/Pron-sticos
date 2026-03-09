import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# Tus aeropuertos
AIRPORTS = {
    "SAEZ": {"lat": -34.82, "lon": -58.53, "usa": False},
    "SBGR": {"lat": -23.43, "lon": -46.47, "usa": False},
    "KMIA": {"lat": 25.79,  "lon": -80.29,  "usa": True},
    "KATL": {"lat": 33.64,  "lon": -84.42,  "usa": True},
    "KLGA": {"lat": 40.77,  "lon": -73.87,  "usa": True}
}

def get_forecast(lat, lon, models_list="auto"):
    """
    Descarga pronóstico horario de temperatura_2m usando Open-Meteo.
    models_list puede ser "auto" o una lista como ["gfs", "icon", "ecmwf", "gem", "hrrr"]
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": ",".join(models_list) if isinstance(models_list, list) else models_list,
        "timezone": "UTC",
        "forecast_days": 3,
        "past_days": 0  # opcional, por si querés extender
    }
    
    try:
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()  # lanza error si no es 200
        data = r.json()
        
        if "hourly" not in data or "time" not in data["hourly"]:
            print("  Respuesta sin datos horarios")
            return None
            
        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature_2m": data["hourly"]["temperature_2m"],
        })
        
        # Si pediste varios modelos, Open-Meteo devuelve solo temperature_2m combinado
        # (blend inteligente). Si querés separar por modelo → tendrías que pedir uno por uno.
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"  Error en API: {e}")
        return None


def main():
    os.makedirs("forecasts", exist_ok=True)
    ahora_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    hoy = datetime.utcnow().date().isoformat()
    
    for icao, info in AIRPORTS.items():
        print(f"Procesando {icao} ({info['lat']}, {info['lon']})...")
        
        # Modelos recomendados (más realistas según docs 2026)
        modelos = ["auto"]  # lo más simple y efectivo (combina lo mejor por ubicación)
        
        # Si querés forzar varios y comparar (más pesado, pero multimodelo explícito)
        # modelos = ["gfs", "icon", "ecmwf", "gem"]
        # if info["usa"]:
        #     modelos.append("hrrr")
        
        df = None
        
        for m in modelos:
            print(f"  Intentando modelo: {m}")
            df_model = get_forecast(info["lat"], info["lon"], m)
            if df_model is not None:
                df_model = df_model.rename(columns={"temperature_2m": f"temp_{m}"})
                if df is None:
                    df = df_model
                else:
                    # Merge por tiempo exacto (inner para evitar NaN si horarios difieren)
                    df = pd.merge(df, df_model, on="time", how="outer")
        
        if df is not None:
            df["descarga_utc"] = ahora_utc
            df["aeropuerto"] = icao
            
            # Reordenar columnas
            cols = ["descarga_utc", "aeropuerto", "time"] + \
                   [c for c in df.columns if c.startswith("temp_")]
            df = df[cols]
            
            # Convertir time a datetime para mejor manejo
            df["time"] = pd.to_datetime(df["time"])
            
            filename = f"forecasts/forecast_{icao.lower()}.csv"
            
            if os.path.isfile(filename):
                # Leer existente y evitar duplicados por hora + descarga
                old = pd.read_csv(filename)
                old["time"] = pd.to_datetime(old["time"])
                # Filtrar solo datos de hoy en adelante para no duplicar histórico
                df_new = df[\~df["time"].isin(old["time"])]
                if not df_new.empty:
                    df_new.to_csv(filename, mode='a', header=False, index=False)
                    print(f"  → Agregadas {len(df_new)} filas nuevas")
                else:
                    print("  → No hay datos nuevos")
            else:
                df.to_csv(filename, index=False)
                print(f"  → Archivo creado con {len(df)} filas")
            
            print(f"✅ {icao} terminado ({len([c for c in df.columns if 'temp_' in c])} columnas de temp)")
        else:
            print(f"❌ {icao} falló (sin datos)")


if __name__ == "__main__":
    main()
