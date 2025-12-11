from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

# Paths
RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
STAGED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = STAGED_DIR / "air_quality_transformed.csv"

# Pollutant columns
POLLUTANTS = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]

# AQI mapping based on PM2.5
def compute_aqi(pm2_5: float) -> str:
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

# Pollution severity score
def compute_severity(row: pd.Series) -> float:
    return (
        (row.get("pm2_5", 0) * 5) +
        (row.get("pm10", 0) * 3) +
        (row.get("nitrogen_dioxide", 0) * 4) +
        (row.get("sulphur_dioxide", 0) * 4) +
        (row.get("carbon_monoxide", 0) * 2) +
        (row.get("ozone", 0) * 3)
    )

# Risk classification
def classify_risk(severity: float) -> str:
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

def flatten_city_json(file_path: Path) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    city_name = file_path.stem.split("_raw_")[0].replace("_", " ").title()

    # The API returns hourly data under "hourly" key
    hourly_data = data.get("hourly", {})
    if not hourly_data:
        return pd.DataFrame()  # skip empty data

    # Determine number of records
    times = hourly_data.get("time", [])
    n_records = len(times)
    if n_records == 0:
        return pd.DataFrame()

    # Build dataframe
    df = pd.DataFrame({"time": times})
    for pollutant in POLLUTANTS:
        df[pollutant] = hourly_data.get(pollutant, [np.nan] * n_records)
    
    df["city"] = city_name

    # Convert time to datetime
    df["time"] = pd.to_datetime(df["time"])

    # Convert pollutants to numeric
    for col in POLLUTANTS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove records where all pollutant readings are missing
    df = df.dropna(subset=POLLUTANTS, how="all")

    # Feature engineering
    df["aqi"] = df["pm2_5"].apply(lambda x: compute_aqi(x) if not pd.isna(x) else np.nan)
    df["severity"] = df.apply(compute_severity, axis=1)
    df["risk"] = df["severity"].apply(classify_risk)
    df["hour"] = df["time"].dt.hour

    return df[["city", "time"] + POLLUTANTS + ["aqi", "severity", "risk", "hour"]]

def main():
    all_files = list(RAW_DIR.glob("*.json"))
    dfs = []

    for f in all_files:
        df_city = flatten_city_json(f)
        if not df_city.empty:
            dfs.append(df_city)

    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        df_all.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Transformed data saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No data to transform.")

