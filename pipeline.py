import os
import json
from datetime import datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ✅ Prefect (para que "main" sea un Flow deployable)
from prefect import flow

# =========================
# Config
# =========================
load_dotenv()

ALPHA_KEY = os.getenv("ALPHAVANTAGE_KEY")
ALPHA_FUNCTION = os.getenv("ALPHAVANTAGE_FUNCTION", "TIME_SERIES_DAILY")
ALPHA_SYMBOL = os.getenv("ALPHAVANTAGE_SYMBOL", "IBM")

STAGING_DIR = os.getenv("STAGING_DIR", "staging")
STAGING_FILENAME = os.getenv("STAGING_FILENAME", "alpha_raw.json")
STAGING_PATH = os.path.join(STAGING_DIR, STAGING_FILENAME)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "Project_API")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "prices_daily")

ALPHA_URL = "https://www.alphavantage.co/query"


# =========================
# 1) Extract: llamar API
# =========================
def fetch_alpha_vantage(function_name: str, symbol: str) -> dict:
    if not ALPHA_KEY:
        raise RuntimeError("Falta ALPHAVANTAGE_KEY (defínela como variable de entorno o en .env local).")

    params = {"function": function_name, "symbol": symbol, "apikey": ALPHA_KEY}

    r = requests.get(ALPHA_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Manejo de errores típicos de Alpha Vantage
    if "Error Message" in data:
        raise RuntimeError(f"AlphaVantage Error: {data['Error Message']}")
    if "Note" in data:
        # rate limit del plan free
        raise RuntimeError(f"AlphaVantage Rate Limit: {data['Note']}")

    return data


# =========================
# 2) Staging: guardar JSON temporal
# =========================
def save_staging_json(raw: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)


def load_staging_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# =========================
# 3) Transform: DataFrame + limpieza básica
# =========================
def alpha_daily_to_dataframe(raw: dict) -> pd.DataFrame:
    """
    Convierte TIME_SERIES_DAILY -> DataFrame con columnas:
    date, open, high, low, close, volume
    """
    series = raw.get("Time Series (Daily)")
    if not series:
        raise RuntimeError("No se encontró 'Time Series (Daily)' en la respuesta.")

    df = pd.DataFrame.from_dict(series, orient="index").reset_index()
    df.rename(columns={"index": "date"}, inplace=True)

    # Renombrar columnas del API
    df.rename(
        columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume",
        },
        inplace=True,
    )

    return df


def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza básica típica:
    - parseo de fechas
    - conversión a numéricos
    - remover nulos/duplicados
    - orden por fecha
    """
    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Convertir numéricos
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Quitar nulos esenciales
    df = df.dropna(subset=["date", "open", "high", "low", "close"])

    # Quitar duplicados por fecha (por si acaso)
    df = df.drop_duplicates(subset=["date"], keep="last")

    # Ordenar por fecha asc
    df = df.sort_values("date").reset_index(drop=True)

    return df


# =========================
# 4) Load: subir a Mongo Atlas
# =========================
def upsert_to_mongo(df: pd.DataFrame, symbol: str) -> int:
    if not MONGO_URI:
        raise RuntimeError("Falta MONGO_URI (defínela como variable de entorno o en .env local).")

    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COLLECTION]

    ingested_at = datetime.now(timezone.utc).isoformat()

    ops = []
    for _, row in df.iterrows():
        doc = {
            "symbol": symbol,
            "date": row["date"].strftime("%Y-%m-%d"),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]) if pd.notna(row["volume"]) else None,
            "source": "AlphaVantage",
            "ingested_at": ingested_at,
        }

        # Upsert por (symbol + date) para no duplicar
        ops.append(
            UpdateOne(
                {"symbol": doc["symbol"], "date": doc["date"]},
                {"$set": doc},
                upsert=True,
            )
        )

    if not ops:
        return 0

    res = coll.bulk_write(ops, ordered=False)
    return int(res.upserted_count + res.modified_count)


# =========================
# Main pipeline (✅ ahora es un Prefect Flow)
# =========================
@flow(name="Project_API_AlphaVantage_Staging_Clean_Mongo")
def main():
    print(f"Extrayendo de Alpha Vantage: {ALPHA_FUNCTION} / {ALPHA_SYMBOL} ...")
    raw = fetch_alpha_vantage(ALPHA_FUNCTION, ALPHA_SYMBOL)

    print(f"Guardando staging JSON en: {STAGING_PATH}")
    save_staging_json(raw, STAGING_PATH)

    print("Cargando staging JSON y creando DataFrame...")
    raw_staging = load_staging_json(STAGING_PATH)
    df = alpha_daily_to_dataframe(raw_staging)

    print("Aplicando limpieza básica...")
    df_clean = basic_cleaning(df)

    print(f"Registros limpios: {len(df_clean)}")
    print("Subiendo a Mongo Atlas (upsert)...")
    written = upsert_to_mongo(df_clean, ALPHA_SYMBOL)

    print(f"OK ✅ Registros escritos/actualizados: {written}")
    print("Listo.")
    return {"records_written_or_updated": written, "symbol": ALPHA_SYMBOL, "staging_path": STAGING_PATH}


if __name__ == "__main__":
    main()
