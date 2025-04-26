# live_inserter.py

import os
import time
import threading
from datetime import datetime, timedelta, timezone
import traceback

import numpy as np
import pandas as pd
import requests
from pymongo import MongoClient
from flask import Flask
from zoneinfo import ZoneInfo  # Python 3.9+ for timezone

# ------------------ Timezones ------------------

IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc

def to_utc(dt):
    """Convert naive or IST datetime to UTC aware datetime."""
    if dt.tzinfo is None:
        # Assume naive dt is in IST, localize then convert
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(UTC)

def to_ist(dt):
    """Convert UTC aware datetime to IST aware datetime."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(IST)

def get_current_ist():
    return datetime.now(IST).replace(second=0, microsecond=0)

def get_current_utc():
    return datetime.now(UTC).replace(second=0, microsecond=0)

# ------------------ MongoDB Setup ------------------

MONGO_URI = os.environ.get("MONGO_URI")
SELF_PING_URL = os.environ.get("SELF_PING_URL")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set. Please set it in environment variables.")
if not SELF_PING_URL:
    print(f"[{get_current_ist()}] WARNING: SELF_PING_URL not set. Self-ping will be disabled.")

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
    raise e

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
        if latest:
            # Convert stored IST naive timestamp to UTC aware
            ts = latest["timestamp"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
            ts_utc = ts.astimezone(UTC)
        else:
            # Default start date localized to IST then converted to UTC
            ts_utc = datetime(2025, 4, 10, tzinfo=IST).astimezone(UTC)
        print(f"[{get_current_ist()}] Last timestamp for {directory} (UTC): {ts_utc}")
        return ts_utc
    except Exception as e:
        print(f"[{get_current_ist()}] ERROR getting last timestamp for {directory}: {e}")
        traceback.print_exc()
        # fallback default
        return datetime(2025, 4, 10, tzinfo=IST).astimezone(UTC)

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

def generate_and_bulk_insert(directory, cfg, start_ts_utc, end_ts_utc, prev_val):
    try:
        # Generate timestamps in IST for realistic intervals, then convert each to UTC
        timestamps_ist = pd.date_range(start=start_ts_utc.astimezone(IST), end=end_ts_utc.astimezone(IST), freq="15min", tz=IST)
        if len(timestamps_ist) == 0:
            print(f"[{get_current_ist()}] WARNING: No timestamps generated for {directory}")
            return prev_val, start_ts_utc - timedelta(minutes=15)

        docs = []
        for ts_ist in timestamps_ist:
            current, added, deleted, updated = generate_value(prev_val, cfg)
            # Store timestamps in UTC (aware) in DB
            ts_utc = ts_ist.astimezone(UTC).replace(tzinfo=None)  # Store naive UTC for MongoDB consistency
            docs.append({
                "timestamp": ts_utc,
                "directory": directory,
                "storage_gb": current,
                "added_gb": added,
                "deleted_gb": deleted,
                "updated_gb": updated
            })
            prev_val = current

        if docs:
            collection.insert_many(docs)
            print(f"[{get_current_ist()}] Inserted {len(docs)} docs for {directory} (UTC timestamps stored)")
            return prev_val, timestamps_ist[-1].astimezone(UTC)
        else:
            print(f"[{get_current_ist()}] No documents to insert for {directory}")
            return prev_val, start_ts_utc - timedelta(minutes=15)
    except Exception as e:
        print(f"[{get_current_ist()}] ERROR in bulk insert for {directory}: {e}")
        traceback.print_exc()
        return prev_val, start_ts_utc - timedelta(minutes=15)

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

    now_ist = get_current_ist()
    now_utc = now_ist.astimezone(UTC)
    print(f"[{now_ist}] Current server time (IST), UTC: {now_utc}")

    # Backfilling phase
    for directory, cfg in profiles.items():
        try:
            last_ts_utc = get_last_timestamp(directory)
            # Find previous storage value at last_ts (stored as naive UTC in DB)
            prev_val_doc = collection.find_one({"directory": directory, "timestamp": last_ts_utc.replace(tzinfo=None)})
            prev_val = prev_val_doc["storage_gb"] if prev_val_doc else cfg["base"]

            start_ts_utc = last_ts_utc + timedelta(minutes=15)
            if start_ts_utc <= now_utc:
                new_prev_val, last_timestamp = generate_and_bulk_insert(directory, cfg, start_ts_utc, now_utc, prev_val)
                last_vals[directory] = new_prev_val
            else:
                last_vals[directory] = prev_val
        except Exception as e:
            traceback.print_exc()
            last_vals[directory] = cfg["base"]

    # Wait until next 15-minute mark in IST
    now_ist = get_current_ist()
    minutes = (now_ist.minute // 15 + 1) * 15
    if minutes == 60:
        next_live_ts_ist = now_ist.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_live_ts_ist = now_ist.replace(minute=minutes, second=0, microsecond=0)

    while get_current_ist() < next_live_ts_ist:
        time.sleep(5)

    print(f"[{get_current_ist()}] ===== ENTERING LIVE MODE =====")
    next_ping_time = get_current_ist() + timedelta(minutes=5)

    while True:
        now_ist = get_current_ist()
        now_utc = now_ist.astimezone(UTC)
        for directory, cfg in profiles.items():
            try:
                prev_val = last_vals[directory]
                current, added, deleted, updated = generate_value(prev_val, cfg)
                # Store timestamps in UTC naive format
                ts_to_store = now_utc.replace(tzinfo=None)
                collection.insert_one({
                    "timestamp": ts_to_store,
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

        next_slot = now_ist + timedelta(minutes=15)
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
