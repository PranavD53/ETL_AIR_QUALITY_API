from __future__ import annotations
import pandas as pd
import numpy as np
import time
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import os

# Config
TRANSFORMED_FILE = Path("data/staged/air_quality_transformed.csv")
BATCH_SIZE = 200
MAX_RETRIES = 2

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Table name
TABLE_NAME = "air_quality_data"

def load_data():
    df = pd.read_csv(TRANSFORMED_FILE)
    print(f"Loaded {len(df)} rows from {TRANSFORMED_FILE}")

    # Convert NaN → None for SQL insert
    df = df.replace({np.nan: None})

    # Convert datetime to ISO format string
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    records = df.to_dict(orient="records")
    total_inserted = 0

    # Batch insert
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                result = supabase.table(TABLE_NAME).insert(batch).execute()
                # In v2, `result.data` contains inserted rows
                inserted_count = len(result.data) if result.data else 0
                total_inserted += inserted_count
                print(f"✅ Inserted batch {i//BATCH_SIZE + 1} ({inserted_count} rows)")
                break
            except Exception as e:
                retries += 1
                print(f"⚠️ Batch {i//BATCH_SIZE + 1} failed (attempt {retries}): {e}")
                time.sleep(2 ** retries)
        else:
            print(f"❌ Failed to insert batch {i//BATCH_SIZE + 1} after {MAX_RETRIES} retries")


    print(f"✅ Total rows inserted: {total_inserted}")
