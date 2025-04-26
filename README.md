# HPE-Data-Generator

# 📊 Storage Data Simulator API

A **FastAPI-based** server that **simulates storage usage data** for different directories (`/scratch`, `/projects`, `/customer`, `/info`) and inserts it into a **MongoDB** database at **15-minute intervals**.  
It supports **backfilling historical data** and **live continuous insertion**.

---

## 🚀 Features

- **Automatic Backfill**: Fills missing storage data since the last available timestamp.
- **Live Data Insertion**: Inserts new records every 15 minutes.
- **Profiles**: Different storage profiles with realistic drift, volatility, spikes, and drops.
- **Manual Trigger**: API to manually run the data generation.
- **Lightweight**: Runs in a background thread using FastAPI's lifespan events.

---

## 🛠️ Tech Stack

- **FastAPI**: Web server and API framework
- **MongoDB**: Database to store generated data
- **Pandas & NumPy**: Data generation and timestamp management
- **Pymongo**: MongoDB interaction
- **Dotenv**: Securely load environment variables
- **Threading**: Background live data generation

---

## 📂 Project Structure

```
.
├── live_inserter.py       # FastAPI app with background data generator
├── .env          # Environment variables for MongoDB connection
└── README.md     # Project documentation
```

---

## ⚙️ Environment Variables

Create a `.env` file in your project root with the following variables:

```
MONGO_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
DB=your_database_name
COLLECTION=your_collection_name
```

---

## 🏃 Getting Started

### 1. Install Dependencies

```
pip install fastapi uvicorn pymongo python-dotenv pandas numpy pytz
```

---

### 2. Run the Server

```
python main.py
```

Server will start on: `http://localhost:10000`

---

## 📡 API Endpoints

| Method | Endpoint         | Description                              |
|:-------|:------------------|:-----------------------------------------|
| GET    | `/keep-alive`      | Health check endpoint (returns alive)    |
| HEAD   | `/keep-alive`      | Health check for head requests           |
| GET    | `/run-cron`        | Manually trigger the data insertion loop |

---

## 🔄 How It Works

- On server startup:
  - **Backfills** any missing data from the last known timestamp up to the current time.
  - **Waits** until the next 15-minute mark.
  - **Begins live insertion**, adding new records every 15 minutes.

- Storage profiles simulate:
  - Gradual increase (drift)
  - Random volatility
  - Occasional spikes (sudden usage)
  - Occasional drops (data deletion)

---

## 🧠 Notes

- Times are generated in **Indian Standard Time (IST)**.
- Data is inserted with **15-minute frequency**.
- Bulk inserts are done during backfill; live inserts are per timestamp.




