from fastapi import FastAPI
from contextlib import asynccontextmanager
from threading import Thread
import time
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import pytz  

# ------------------ Setup ------------------
app = FastAPI()

# ------------------ MongoDB Setup ------------------
load_dotenv()  
MONGO_URI = os.environ.get("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client[os.environ.get("DB")]
collection = db[os.environ.get("COLLECTION")]

# ------------------ Timezone Setup ------------------
IST = pytz.timezone('Asia/Kolkata')

def get_indian_time():
    """Return current Indian time without timezone info."""
    return datetime.now(IST).replace(tzinfo=None, second=0, microsecond=0)

# ------------------ Storage Profiles ------------------
profiles = {
    "/scratch":   {"base": 1500, "volatility": 6.5, "drift": 0.008, "spike": 0.004, "drop": 0.003},
    "/projects":  {"base": 900,  "volatility": 4.0, "drift": 0.0055, "spike": 0.0025, "drop": 0.002},
    "/customer":  {"base": 600,  "volatility": 2.0, "drift": 0.003, "spike": 0.0015, "drop": 0.001},
    "/info":      {"base": 400,  "volatility": 1.2, "drift": 0.0018, "spike": 0.001, "drop": 0.0008},
}

# ------------------ Utility Functions ------------------
def get_last_timestamp(directory):
    """Fetch latest timestamp for a directory, or start from April 10, 2025."""
    doc = collection.find({"directory": directory}).sort("timestamp", -1).limit(1)
    latest = next(doc, None)
    return latest["timestamp"] if latest else datetime(2025, 4, 10)

def generate_value(prev_val, cfg):
    """Generate next storage value with drift, volatility, spike/drop."""
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
    """Generate and bulk insert historical data to backfill."""
    timestamps = pd.date_range(start=start_ts, end=end_ts, freq="15min")
    docs = []
    for ts in timestamps:
        ts = ts.to_pydatetime()
        ts = ts.replace(tzinfo=None)  # 👈 Remove timezone info if present
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
        return prev_val, timestamps[-1].to_pydatetime()
    return prev_val, start_ts - timedelta(minutes=15)

# ------------------ Background Data Insertion Loop ------------------
def live_data_insertion_loop():
    """Main process: backfill missing data and continue live insertion every 15 min."""
    last_vals = {}
    now = get_indian_time()

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
            print(f"🟡 {directory} already up to date.")
            last_vals[directory] = prev_val
    print("Backfill complete successfully")

    # Wait until next 15-min slot
    minutes = (now.minute // 15 + 1) * 15
    if minutes == 60:
        next_live_ts = now.replace(minute=0) + timedelta(hours=1)
    else:
        next_live_ts = now.replace(minute=minutes)

    print(f"Waiting until {next_live_ts} to start live mode...")
    while get_indian_time() < next_live_ts:
        time.sleep(5)

    print("Entering live mode (insert every 15 min)...")

    # Live data insertion every 15 minutes
    while True:
        now = get_indian_time()
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
        print(f"[{now}]  Inserted live records.")
        time.sleep(900)  # Sleep 15 minutes

# ------------------ Lifespan with Startup ------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Context manager for FastAPI lifespan."""
    # Run startup tasks
    data_insertion_thread = Thread(target=live_data_insertion_loop)
    data_insertion_thread.daemon = True
    data_insertion_thread.start()
    
    yield  


app = FastAPI(lifespan=lifespan)

@app.get("/keep-alive")
@app.head("/keep-alive")  
async def keep_alive():
    """Endpoint to keep the server alive."""
    return {"status": "alive"}

@app.get("/run-cron")
async def run_cron():
    """Manually trigger the cron job."""
    live_data_insertion_loop()
    return {"status": "data insertion triggered"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)