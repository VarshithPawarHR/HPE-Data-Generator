# live_inserter.py

import os
import time
import threading
from datetime import datetime, timedelta
import traceback

import numpy as np
import pandas as pd
import requests
from pymongo import MongoClient
from flask import Flask
from zoneinfo import ZoneInfo  # For proper timezone management (Python 3.9+)

# ------------------ MongoDB Setup ------------------

MONGO_URI = os.environ.get("MONGO_URI")
SELF_PING_URL = os.environ.get("SELF_PING_URL")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set. Please set it in Render environment variables.")
if not SELF_PING_URL:
    print(f"[{datetime.now()}] WARNING: SELF_PING_URL not set. Self-ping will be disabled.")

# Helper to get current IST time
def get_current_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).replace(second=0, microsecond=0)

print(f"[{get_current_ist()}] Connecting to MongoDB...")
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print(f"[{get_current_ist()}] MongoDB connection successful!")
    db = client["storage_simulation"]
    collection = db["usage_logs"]
except Exception as e:
    print(f"[{get_current_ist()}] CRITICAL ERROR: MongoDB connection failed: {e}")
    traceback.print_exc()

# ------------------ Storage Profiles ------------------

profiles = {
    "/scratch":   {"base": 1500, "volatility": 6.5, "drift": 0.008, "spike": 0.004, "drop": 0.003},
    "/projects":  {"base": 900,  "volatility": 4.0, "drift": 0.0055, "spike": 0.0025, "drop": 0.002},
    "/customer":  {"base": 600,  "volatility": 2.0, "drift": 0.003, "spike": 0.0015, "drop": 0.001},
    "/info":      {"base": 400,  "volatility": 1.2, "drift": 0.0018, "spike": 0.001, "drop": 0.0008},
}

# ------------------ Utility Functions ------------------

def get_last_timestamp(directory):
    try:
        doc = collection.find({"directory": directory}).sort("timestamp", -1).limit(1)
        latest = next(doc, None)
        result = latest["timestamp"] if latest else datetime(2025, 4, 10, tzinfo=ZoneInfo("Asia/Kolkata"))
        print(f"[{get_current_ist()}] Last timestamp for {directory}: {result}")
        return result
    except Exception as e:
        print(f"[{get_current_ist()}] ERROR getting last timestamp for {directory}: {e}")
        traceback.print_exc()
        return datetime(2025, 4, 10, tzinfo=ZoneInfo("Asia/Kolkata"))

def generate_value(prev_val, cfg):
    try:
        drift = np.random.normal(cfg["drift"], cfg["drift"] * 0.25)
        change = np.random.normal(0, cfg["volatility"])
        if np.random.rand() < cfg["spike"]:
            change += np.random.uniform(10, 60)
        if np.random.rand() < cfg["drop"]:
            change -= np.random.uniform(5, 80)
        new_val = round(max(prev_val + drift + change, 0), 2)
        delta = new_val - prev_val
        return new_val, round(max(delta, 0), 2), round(max(-delta, 0), 2), round(abs(delta), 2)
    except Exception as e:
        print(f"[{get_current_ist()}] ERROR generating value: {e}")
        traceback.print_exc()
        return prev_val + 0.01, 0.01, 0, 0.01

def generate_and_bulk_insert(directory, cfg, start_ts, end_ts, prev_val):
    try:
        print(f"[{get_current_ist()}] Generating data for {directory} from {start_ts} to {end_ts}")
        timestamps = pd.date_range(start=start_ts, end=end_ts, freq="15min", tz="Asia/Kolkata")
        if len(timestamps) == 0:
            print(f"[{get_current_ist()}] WARNING: No timestamps generated for {directory}")
            return prev_val, start_ts - timedelta(minutes=15)

        docs = []
        for ts in timestamps:
            current, added, deleted, updated = generate_value(prev_val, cfg)
            docs.append({
                "timestamp": ts.to_pydatetime(),  # Convert Timestamp to datetime
                "directory": directory,
                "storage_gb": current,
                "added_gb": added,
                "deleted_gb": deleted,
                "updated_gb": updated
            })
            prev_val = current

        if docs:
            collection.insert_many(docs)
            print(f"[{get_current_ist()}] Successfully inserted {len(docs)} documents for {directory}")
            return prev_val, timestamps[-1].to_pydatetime()
        else:
            print(f"[{get_current_ist()}] No documents to insert for {directory}")
            return prev_val, start_ts - timedelta(minutes=15)
    except Exception as e:
        print(f"[{get_current_ist()}] ERROR in bulk insert for {directory}: {e}")
        traceback.print_exc()
        return prev_val, start_ts - timedelta(minutes=15)

def ping_self():
    if not SELF_PING_URL:
        return True
    try:
        response = requests.get(SELF_PING_URL, timeout=5)
        return True
    except Exception as e:
        traceback.print_exc()
        return True

# ------------------ Main Insertion Logic ------------------

def live_data_insertion_loop():
    print(f"[{get_current_ist()}] ===== SERVICE STARTED =====")
    last_vals = {}

    try:
        ping_thread = threading.Thread(target=ping_self)
        ping_thread.daemon = True
        ping_thread.start()
        ping_thread.join(timeout=10)
    except:
        pass

    now = get_current_ist()
    print(f"[{get_current_ist()}] Current server time (IST): {now}")

    # Backfilling
    for directory, cfg in profiles.items():
        try:
            last_ts = get_last_timestamp(directory)
            prev_val_doc = collection.find_one({"directory": directory, "timestamp": last_ts})
            prev_val = prev_val_doc["storage_gb"] if prev_val_doc else cfg["base"]

            start_ts = last_ts + timedelta(minutes=15)
            if start_ts <= now:
                new_prev_val, last_timestamp = generate_and_bulk_insert(directory, cfg, start_ts, now, prev_val)
                last_vals[directory] = new_prev_val
            else:
                last_vals[directory] = prev_val
        except Exception as e:
            traceback.print_exc()
            last_vals[directory] = cfg["base"]

    # Wait until next 15-minute mark
    now = get_current_ist()
    minutes = (now.minute // 15 + 1) * 15
    if minutes == 60:
        next_live_ts = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_live_ts = now.replace(minute=minutes, second=0, microsecond=0)

    while get_current_ist() < next_live_ts:
        time.sleep(5)

    print(f"[{get_current_ist()}] ===== ENTERING LIVE MODE =====")
    next_ping_time = get_current_ist() + timedelta(minutes=5)

    while True:
        now = get_current_ist()
        for directory, cfg in profiles.items():
            try:
                prev_val = last_vals[directory]
                current, added, deleted, updated = generate_value(prev_val, cfg)
                collection.insert_one({
                    "timestamp": now,
                    "directory": directory,
                    "storage_gb": current,
                    "added_gb": added,
                    "deleted_gb": deleted,
                    "updated_gb": updated
                })
                last_vals[directory] = current
            except Exception as e:
                traceback.print_exc()

        if get_current_ist() >= next_ping_time:
            threading.Thread(target=ping_self, daemon=True).start()
            next_ping_time = get_current_ist() + timedelta(minutes=5)

        next_slot = now + timedelta(minutes=15)
        sleep_time = (next_slot - get_current_ist()).total_seconds()
        if sleep_time <= 0:
            sleep_time = 15 * 60

        time.sleep(sleep_time)

# ------------------ Flask App Setup ------------------

app = Flask(__name__)

@app.route('/')
def home():
    return "Storage simulation service is running."

@app.route('/status')
def status():
    try:
        db.command('ping')
        return "Service is healthy. MongoDB connection is working."
    except Exception as e:
        return f"Service is running but MongoDB connection is failing: {str(e)}", 500

@app.route('/helpline')
def helpline():
    return "OK", 200

# ------------------ Entry Point ------------------

if __name__ == "__main__":
    insertion_thread = threading.Thread(target=live_data_insertion_loop, daemon=True)
    insertion_thread.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
