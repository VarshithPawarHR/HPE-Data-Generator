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

# ------------------ MongoDB Setup ------------------

MONGO_URI = os.environ.get("MONGO_URI")
SELF_PING_URL = os.environ.get("SELF_PING_URL")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set. Please set it in Render environment variables.")
if not SELF_PING_URL:
    print(f"[{datetime.now()}] WARNING: SELF_PING_URL not set. Self-ping will be disabled.")

print(f"[{datetime.now()}] Connecting to MongoDB...")
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Test connection
    client.admin.command('ping')
    print(f"[{datetime.now()}] MongoDB connection successful!")
    db = client["storage_simulation"]
    collection = db["usage_logs"]
except Exception as e:
    print(f"[{datetime.now()}] CRITICAL ERROR: MongoDB connection failed: {e}")
    traceback.print_exc()
    # Don't exit, let it fail later with more context

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
        result = latest["timestamp"] if latest else datetime(2025, 4, 10)
        print(f"[{datetime.now()}] Last timestamp for {directory}: {result}")
        return result
    except Exception as e:
        print(f"[{datetime.now()}] ERROR getting last timestamp for {directory}: {e}")
        traceback.print_exc()
        return datetime(2025, 4, 10)  # Default fallback

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
        print(f"[{datetime.now()}] ERROR generating value: {e}")
        traceback.print_exc()
        # Return safe values if calculation fails
        return prev_val + 0.01, 0.01, 0, 0.01

def generate_and_bulk_insert(directory, cfg, start_ts, end_ts, prev_val):
    try:
        print(f"[{datetime.now()}] Generating data for {directory} from {start_ts} to {end_ts}")
        timestamps = pd.date_range(start=start_ts, end=end_ts, freq="15min")
        if len(timestamps) == 0:
            print(f"[{datetime.now()}] WARNING: No timestamps generated for {directory}")
            return prev_val, start_ts - timedelta(minutes=15)
        
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
            print(f"[{datetime.now()}] Inserting {len(docs)} documents for {directory}")
            collection.insert_many(docs)
            print(f"[{datetime.now()}] Successfully inserted {len(docs)} documents for {directory}")
            return prev_val, timestamps[-1]
        else:
            print(f"[{datetime.now()}] No documents to insert for {directory}")
            return prev_val, start_ts - timedelta(minutes=15)
    except Exception as e:
        print(f"[{datetime.now()}] ERROR in bulk insert for {directory}: {e}")
        traceback.print_exc()
        return prev_val, start_ts - timedelta(minutes=15)

def ping_self():
    if not SELF_PING_URL:
        print(f"[{datetime.now()}] Self-ping skipped: URL not configured.")
        return True
        
    try:
        print(f"[{datetime.now()}] Attempting self-ping at {SELF_PING_URL}")
        response = requests.get(SELF_PING_URL, timeout=5)
        print(f"[{datetime.now()}] Self-ping {'success' if response.status_code == 200 else 'fail'}: {response.status_code}")
        return True
    except Exception as e:
        print(f"[{datetime.now()}] Self-ping error: {e}")
        traceback.print_exc()
        # Return True anyway to prevent this from blocking execution
        return True

# ------------------ Main Insertion Logic ------------------

def live_data_insertion_loop():
    print(f"[{datetime.now()}] ===== SERVICE STARTED =====")
    last_vals = {}

    # First ping to check connectivity, but don't block on failure
    try:
        print(f"[{datetime.now()}] Testing self-ping functionality...")
        ping_thread = threading.Thread(target=ping_self)
        ping_thread.daemon = True
        ping_thread.start()
        # Set a timeout for the self-ping thread
        ping_thread.join(timeout=10)
        if ping_thread.is_alive():
            print(f"[{datetime.now()}] WARNING: Self-ping timed out after 10 seconds. Continuing anyway.")
    except Exception as e:
        print(f"[{datetime.now()}] ERROR during self-ping test: {e}")
        traceback.print_exc()
        # Continue execution regardless of self-ping results
    
    print(f"[{datetime.now()}] Proceeding with normal execution.")
    now = datetime.now().replace(second=0, microsecond=0)
    print(f"[{datetime.now()}] Current server time: {now}")

    print(f"[{datetime.now()}] Backfilling missing data...")
    for directory, cfg in profiles.items():
        try:
            print(f"[{datetime.now()}] Processing {directory}...")
            last_ts = get_last_timestamp(directory)
            prev_val_doc = collection.find_one({"directory": directory, "timestamp": last_ts})
            prev_val = prev_val_doc["storage_gb"] if prev_val_doc else cfg["base"]
            print(f"[{datetime.now()}] Previous value for {directory}: {prev_val}")

            start_ts = last_ts + timedelta(minutes=15)
            if start_ts <= now:
                print(f"[{datetime.now()}] Backfilling {directory} from {start_ts} to {now}")
                new_prev_val, last_timestamp = generate_and_bulk_insert(directory, cfg, start_ts, now, prev_val)
                last_vals[directory] = new_prev_val
                print(f"[{datetime.now()}] Backfill complete for {directory}. New value: {new_prev_val}, Last timestamp: {last_timestamp}")
            else:
                print(f"[{datetime.now()}] {directory} already up to date.")
                last_vals[directory] = prev_val
        except Exception as e:
            print(f"[{datetime.now()}] ERROR processing {directory}: {e}")
            traceback.print_exc()
            last_vals[directory] = cfg["base"]
    
    print(f"[{datetime.now()}] Backfill complete. Current state: {last_vals}")

    try:
        # Wait until next 15-min slot
        now = datetime.now()
        minutes = (now.minute // 15 + 1) * 15
        if minutes == 60:
            next_live_ts = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            next_live_ts = now.replace(minute=minutes, second=0, microsecond=0)

        print(f"[{datetime.now()}] Waiting until {next_live_ts} to enter live mode...")
        wait_count = 0
        
        while datetime.now() < next_live_ts:
            if wait_count % 12 == 0:  # Report every minute
                print(f"[{datetime.now()}] Still waiting for next slot... ({(next_live_ts - datetime.now()).total_seconds():.0f} seconds left)")
            time.sleep(5)
            wait_count += 1

        print(f"[{datetime.now()}] ===== ENTERING LIVE MODE =====")
        next_ping_time = datetime.now() + timedelta(minutes=5)
        
        while True:
            try:
                now = datetime.now().replace(second=0, microsecond=0)
                print(f"[{datetime.now()}] Generating live data for timestamp: {now}")
                
                for directory, cfg in profiles.items():
                    try:
                        prev_val = last_vals[directory]
                        current, added, deleted, updated = generate_value(prev_val, cfg)
                        
                        print(f"[{datetime.now()}] Inserting for {directory}: {current} GB")
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
                        print(f"[{datetime.now()}] ERROR processing live data for {directory}: {e}")
                        traceback.print_exc()

                print(f"[{datetime.now()}] Inserted new live records.")

                # Self-ping every 5 mins in a non-blocking way
                if datetime.now() >= next_ping_time:
                    ping_thread = threading.Thread(target=ping_self)
                    ping_thread.daemon = True
                    ping_thread.start()
                    next_ping_time = datetime.now() + timedelta(minutes=5)

                # Sleep until next 15-minute mark
                next_slot = now + timedelta(minutes=15)
                sleep_time = (next_slot - datetime.now()).total_seconds()
                if sleep_time <= 0:
                    sleep_time = 15 * 60  # Default to 15 minutes if calculation fails
                
                print(f"[{datetime.now()}] Sleeping for {sleep_time:.2f} seconds (until {next_slot})...")
                time.sleep(sleep_time)
                
            except Exception as e:
                print(f"[{datetime.now()}] ERROR in live insertion loop: {e}")
                traceback.print_exc()
                time.sleep(60)  # Wait a minute before retrying
                
    except Exception as e:
        print(f"[{datetime.now()}] CRITICAL ERROR in main loop: {e}")
        traceback.print_exc()

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
    """Endpoint for self-ping to call"""
    return "OK", 200

# ------------------ Entry Point ------------------

if __name__ == "__main__":
    # Start data insertion thread
    insertion_thread = threading.Thread(target=live_data_insertion_loop, daemon=True)
    insertion_thread.start()

    # Start Flask app
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)