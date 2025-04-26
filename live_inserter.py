import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from pymongo import MongoClient

# ------------------ MongoDB Setup ------------------

MONGO_URI = os.environ.get("MONGO_URI")
SELF_PING_URL = os.environ.get("SELF_PING_URL")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set. Please set it in Render environment variables.")
if not SELF_PING_URL:
    raise RuntimeError("SELF_PING_URL not set. Please set your service URL for self-ping.")

client = MongoClient(MONGO_URI)
db = client["storage_simulation"]
collection = db["usage_logs"]

# ------------------ Storage Profiles ------------------

profiles = {
    "/scratch":   {"base": 1500, "volatility": 6.5, "drift": 0.008, "spike": 0.004, "drop": 0.003},
    "/projects":  {"base": 900,  "volatility": 4.0, "drift": 0.0055, "spike": 0.0025, "drop": 0.002},
    "/customer":  {"base": 600,  "volatility": 2.0, "drift": 0.003, "spike": 0.0015, "drop": 0.001},
    "/info":      {"base": 400,  "volatility": 1.2, "drift": 0.0018, "spike": 0.001, "drop": 0.0008},
}

# ------------------ Utility Functions ------------------

def get_last_timestamp(directory):
    doc = collection.find({"directory": directory}).sort("timestamp", -1).limit(1)
    latest = next(doc, None)
    return latest["timestamp"] if latest else datetime(2025, 4, 10)

def generate_value(prev_val, cfg):
    drift = np.random.normal(cfg["drift"], cfg["drift"] * 0.25)
    change = np.random.normal(0, cfg["volatility"])
    if np.random.rand() < cfg["spike"]:
        change += np.random.uniform(10, 60)
    if np.random.rand() < cfg["drop"]:
        change -= np.random.uniform(5, 80)
    new_val = round(max(prev_val + drift + change, 0), 2)
    delta = new_val - prev_val
    return new_val, round(max(delta, 0), 2), round(max(-delta, 0), 2), round(abs(delta), 2)

def generate_and_bulk_insert(directory, cfg, start_ts, end_ts, prev_val):
    timestamps = pd.date_range(start=start_ts, end=end_ts, freq="15min")
    docs = []
    for ts in timestamps:
        current, added, deleted, updated = generate_value(prev_val, cfg)
        docs.append({
            "timestamp": ts,
            "directory": directory,
            "storage_gb": current,
            "added_gb": added,
            "deleted_gb": deleted,
            "updated_gb": updated
        })
        prev_val = current
    if docs:
        collection.insert_many(docs)
        return prev_val, timestamps[-1]
    return prev_val, start_ts - timedelta(minutes=15)

def ping_self():
    """Send an HTTP request to prevent Render service from sleeping."""
    try:
        response = requests.get(SELF_PING_URL, timeout=5)
        if response.status_code == 200:
            print(f"Self-ping success [{datetime.now()}]")
        else:
            print(f"Self-ping failed: status {response.status_code}")
    except Exception as e:
        print(f"Self-ping error: {e}")

# ------------------ Main Insertion Loop ------------------

def live_data_insertion_loop():
    last_vals = {}
    now = datetime.now().replace(second=0, microsecond=0)

    print("Backfilling missing data...")
    for directory, cfg in profiles.items():
        last_ts = get_last_timestamp(directory)
        prev_val_doc = collection.find_one({"directory": directory, "timestamp": last_ts})
        prev_val = prev_val_doc["storage_gb"] if prev_val_doc else cfg["base"]

        start_ts = last_ts + timedelta(minutes=15)
        if start_ts <= now:
            new_prev_val, _ = generate_and_bulk_insert(directory, cfg, start_ts, now, prev_val)
            last_vals[directory] = new_prev_val
        else:
            print(f"{directory} already up to date.")
            last_vals[directory] = prev_val
    print("Backfill complete.")

    minutes = (now.minute // 15 + 1) * 15
    if minutes == 60:
        next_live_ts = now.replace(minute=0) + timedelta(hours=1)
    else:
        next_live_ts = now.replace(minute=minutes)

    print(f"Waiting until {next_live_ts} to start live mode...")
    while datetime.now() < next_live_ts:
        time.sleep(5)

    print("Entering live mode (insert every 15 min)...")

    next_ping_time = datetime.now() + timedelta(minutes=5)

    while True:
        now = datetime.now().replace(second=0, microsecond=0)
        for directory, cfg in profiles.items():
            prev_val = last_vals[directory]
            current, added, deleted, updated = generate_value(prev_val, cfg)
            doc = {
                "timestamp": now,
                "directory": directory,
                "storage_gb": current,
                "added_gb": added,
                "deleted_gb": deleted,
                "updated_gb": updated
            }
            collection.insert_one(doc)
            last_vals[directory] = current

        print(f"[{now}] Inserted live records.")

        if datetime.now() >= next_ping_time:
            ping_self()
            next_ping_time = datetime.now() + timedelta(minutes=5)

        time.sleep(900)

# ------------------ Entry ------------------

if __name__ == "__main__":
    live_data_insertion_loop()
