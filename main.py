import os
import datetime
import pytz
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# CONFIGURATION
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.environ.get("MONGO_DB", "monitoring")
COLLECTION_NAME = os.environ.get("MONGO_COLLECTION", "directory_stats")

MONITOR_DIRS = os.environ.get("MONITOR_DIRS", "")
DIRS = [d.strip() for d in MONITOR_DIRS.split(",") if d.strip()]

if not DIRS:
    print("No directories specified in MONITOR_DIRS environment variable. Exiting.")
    exit(1)

def dir_exists(path):
    return os.path.isdir(path)

def get_directory_snapshot(directory):
    """Returns a dict of file paths and their sizes in KB, and total size."""
    snapshot = {}
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(directory):
            for f in filenames:
                try:
                    fp = os.path.join(dirpath, f)
                    if os.path.exists(fp):
                        size = os.path.getsize(fp) / 1024
                        snapshot[fp] = size
                        total_size += size
                except Exception as e:
                    print(f"Error processing file {fp}: {e}")
    except Exception as e:
        print(f"Error walking directory {directory}: {e}")
    return snapshot, total_size

def get_utc_datetime_iso():
    """Returns current UTC datetime as ISO string."""
    ist = pytz.timezone("Asia/Kolkata")
    utc = pytz.utc
    ist_time = datetime.datetime.now(ist)
    utc_time = ist_time.astimezone(utc).replace(microsecond=0)
    return utc_time.isoformat() + "Z"

def mongo_connect():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        return collection
    except Exception as e:
        print(f"Could not connect to MongoDB: {e}")
        raise

def check_directories(dirs):
    missing = []
    for directory in dirs:
        if not dir_exists(directory):
            missing.append(directory)
    return missing

def compare_snapshots(prev_snapshot, curr_snapshot):
    """Compare two snapshots to identify added, deleted, and modified files."""
    added = {}
    deleted = {}
    modified = {}

    for file_path, prev_size in prev_snapshot.items():
        if file_path not in curr_snapshot:
            deleted[file_path] = prev_size
        elif abs(curr_snapshot[file_path] - prev_size) > 0.01:
            modified[file_path] = {
                "old_size_kb": prev_size,
                "new_size_kb": curr_snapshot[file_path]
            }

    for file_path, curr_size in curr_snapshot.items():
        if file_path not in prev_snapshot:
            added[file_path] = curr_size

    return added, deleted, modified

def main():
    missing_dirs = check_directories(DIRS)
    if missing_dirs:
        print(f"The following directories do not exist: {missing_dirs}. Exiting.")
        exit(1)

    try:
        collection = mongo_connect()
    except Exception:
        print("Could not connect to MongoDB.")
        exit(1)

    prev_snapshots = {}
    prev_sizes = {}
    for directory in DIRS:
        try:
            snapshot, total_size = get_directory_snapshot(directory)
            prev_snapshots[directory] = snapshot
            prev_sizes[directory] = total_size
        except Exception as e:
            print(f"Error initializing snapshot for {directory}: {e}")
            prev_snapshots[directory] = {}
            prev_sizes[directory] = 0.0

    print("Monitoring started.")

    for directory in DIRS:
        try:
            curr_snapshot, curr_size = get_directory_snapshot(directory)
            prev_snapshot = prev_snapshots.get(directory, {})
            prev_size = prev_sizes.get(directory, 0.0)

            added_files, deleted_files, modified_files = compare_snapshots(prev_snapshot, curr_snapshot)

            added_kb = sum(added_files.values())
            deleted_kb = sum(deleted_files.values())
            updated_kb = sum(
                abs(m["new_size_kb"] - m["old_size_kb"])
                for m in modified_files.values()
            )

            doc = {
                "timestamp": get_utc_datetime_iso(),
                "directory": directory,
                "storage_kb": round(curr_size, 2),
                "added_kb": round(added_kb, 2),
                "deleted_kb": round(deleted_kb, 2),
                "updated_kb": round(updated_kb, 2)
            }

            collection.insert_one(doc)
            print(f"Logged data for {directory}: {doc}")

            prev_snapshots[directory] = curr_snapshot
            prev_sizes[directory] = curr_size

        except Exception as e:
            print(f"Error monitoring {directory}: {e}")

if __name__ == "__main__":
    main()
