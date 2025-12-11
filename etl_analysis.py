from __future__ import annotations
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client, Client
from pathlib import Path
from dotenv import load_dotenv
import os

# Paths
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLE_NAME = "air_quality_data"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_data() -> pd.DataFrame:
    response = supabase.table(TABLE_NAME).select("*").execute()
    data = response.data
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    return df

def kpi_metrics(df: pd.DataFrame):
    # City with highest avg PM2.5
    city_pm25 = df.groupby("city")["pm2_5"].mean().idxmax()
    # City with highest severity
    city_severity = df.groupby("city")["severity"].mean().idxmax()
    # Risk percentages
    risk_dist = df["risk"].value_counts(normalize=True) * 100
    # Hour with worst AQI (PM2.5 avg)
    hour_worst_aqi = df.groupby(df["time"].dt.hour)["pm2_5"].mean().idxmax()

    summary = {
        "highest_avg_pm2_5": city_pm25,
        "highest_severity_city": city_severity,
        "high_risk_pct": risk_dist.get("High Risk", 0),
        "moderate_risk_pct": risk_dist.get("Moderate Risk", 0),
        "low_risk_pct": risk_dist.get("Low Risk", 0),
        "worst_aqi_hour": hour_worst_aqi
    }

    pd.DataFrame([summary]).to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)
    pd.DataFrame(risk_dist).rename(columns={"risk": "percentage"}).to_csv(PROCESSED_DIR / "city_risk_distribution.csv")
    print(f"✅ KPI metrics saved to {PROCESSED_DIR}")

def city_pollution_trends(df: pd.DataFrame):
    trend_cols = ["time", "pm2_5", "pm10", "ozone", "city"]
    df_trends = df[trend_cols]
    df_trends.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)
    print(f"✅ Pollution trends saved to {PROCESSED_DIR}")

def visualizations(df: pd.DataFrame):
    # Histogram PM2.5
    plt.figure(figsize=(8,5))
    df["pm2_5"].hist(bins=30, color="skyblue")
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Frequency")
    plt.savefig(PROCESSED_DIR / "pm2_5_histogram.png")
    plt.close()

    # Bar chart: risk flags per city
    plt.figure(figsize=(10,6))
    df.groupby("city")["risk"].value_counts().unstack().plot(kind="bar", stacked=True)
    plt.title("Risk Flags per City")
    plt.xlabel("City")
    plt.ylabel("Count")
    plt.savefig(PROCESSED_DIR / "risk_flags_per_city.png")
    plt.close()

    # Line chart: hourly PM2.5 trends
    plt.figure(figsize=(12,6))
    for city, group in df.groupby("city"):
        plt.plot(group["time"], group["pm2_5"], label=city)
    plt.title("Hourly PM2.5 Trends")
    plt.xlabel("Time")
    plt.ylabel("PM2.5")
    plt.legend()
    plt.savefig(PROCESSED_DIR / "pm2_5_hourly_trends.png")
    plt.close()

    # Scatter: severity vs PM2.5
    plt.figure(figsize=(8,6))
    plt.scatter(df["pm2_5"], df["severity"], alpha=0.6)
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.savefig(PROCESSED_DIR / "severity_vs_pm2_5.png")
    plt.close()

    print(f"✅ Visualizations saved to {PROCESSED_DIR}")

def run_analysis():
    df = fetch_data()
    kpi_metrics(df)
    city_pollution_trends(df)
    visualizations(df)
