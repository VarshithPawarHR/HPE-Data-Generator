# Directory Storage Monitoring Script

This script (`main.py`) tracks storage usage in directories and saves data to MongoDB. It logs total size, added, deleted, and updated files (in KB) with UTC timestamps (from IST).

## Project Files

- `observing/` folder:
  - `main.py`: The script.
  - `.env`: Settings file.
  - `requirements.txt`: List of Python packages.
  - `.venv/`: Virtual environment (created during setup).

## IMPORTANT: Folder Location

**The `observing` folder MUST be at the root level, same as the directories you monitor (e.g., `customer`, `data`, `projects`, `results`).**  
Example:  
- `/home/pawar/customer`  
- `/home/pawar/data`  
- `/home/pawar/projects`  
- `/home/pawar/results`  
- `/home/pawar/observing` (place it here!)  
**Do NOT put `observing` inside another folder like `customer` or `data`. It must be at the same level.**

## Before You Start

1. **Python 3.6+**: Install from [python.org](https://www.python.org/downloads/). Check: `python --version`.
2. **MongoDB**: Use local MongoDB (install [here](https://www.mongodb.com/docs/manual/installation/)) or cloud (e.g., MongoDB Atlas). Ensure it’s running.
3. **pip**: Python package installer. Check: `pip --version`.

## Setup Steps

1. **Go to the `observing` Folder**:
   ```bash
   cd observing
   ```

2. **Create Virtual Environment**:
   ```bash
   python -m venv .venv
   ```
   Activate it:  
   - Windows: `.venv\Scripts\activate`  
   - macOS/Linux: `source .venv/bin/activate`

3. **Install Packages**:
   ```bash
   pip install -r requirements.txt
   ```
   This installs `pymongo`, `python-dotenv`, `pytz`.

## Settings

1. **Edit `.env` File**:
   Open `.env` in a text editor. Add these:  
   ```
   MONGO_URI=mongodb+srv://<your-username>:<your-password>@<your-cluster>.mongodb.net/?retryWrites=true&w=majority
   MONGO_DB=test
   MONGO_COLLECTION=hello
   MONITOR_DIRS=/home/pawar/customer,/home/pawar/data,/home/pawar/projects,/home/pawar/results
   ```
   - `MONGO_URI`: Your MongoDB connection (local: `mongodb://localhost:27017/`; cloud: see above).
   - `MONGO_DB`: Database name (e.g., `test`).
   - `MONGO_COLLECTION`: Collection name (e.g., `hello`).
   - `MONITOR_DIRS`: Directories to monitor (e.g., `/home/pawar/customer,/home/pawar/data`).

2. **Check MongoDB**:
   Ensure MongoDB is running. Test connection with your `MONGO_URI`.

## How to Run

1. **Activate Virtual Environment**:
   - Windows: `.venv\Scripts\activate`  
   - macOS/Linux: `source .venv/bin/activate`

2. **Run the Script**:
   ```bash
   python main.py
   ```
   It monitors every 5 minutes and logs to MongoDB. Example output:  
   ```
   Monitoring started.
   Logged data for /home/pawar/customer: {'timestamp': '2025-06-05T11:08:00.000Z', 'directory': '/home/pawar/customer', 'storage_kb': 1499.11, 'added_kb': 0, 'deleted_kb': 0.89, 'updated_kb': 0.89}
   ```

3. **Stop**:
   Press `Ctrl+C`.

## What It Does

- Monitors directories in `MONITOR_DIRS` every 15 minutes.
- Tracks:
  - `storage_kb`: Total size (KB).
  - `added_kb`: New files (KB).
  - `deleted_kb`: Deleted files (KB).
  - `updated_kb`: Changed files (KB).
- Saves to MongoDB (`MONGO_DB`, `MONGO_COLLECTION`).
- Example data:  
  ```json
  {
      "timestamp": "2025-06-05T11:08:00.000Z",
      "directory": "/home/pawar/customer",
      "storage_kb": 1499.11,
      "added_kb": 0,
      "deleted_kb": 0.89,
      "updated_kb": 0.89
  }
  ```
- Timestamps: Converts IST to UTC (e.g., 04:38 PM IST = `2025-06-05T11:08:00.000Z`).

