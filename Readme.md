# 🪙 Analyzing Crypto Pipeline

An end-to-end Data Engineering ETL (Extract, Transform, Load) pipeline that fetches real-time cryptocurrency market data, processes it, and stores it in a Data Warehouse for analytics.

## 🏗️ Architecture

The pipeline follows a modern data stack architecture:

1. **Extract**: Fetches the top 20 cryptocurrencies (by market cap) from the CoinGecko API.
2. **Raw Storage (Data Lake)**: Saves the raw JSON responses into a MinIO bucket (`crypto-raw-data`) partitioned by date/time.
3. **Transform**: Reads the latest JSON from MinIO, cleans the data, selects relevant columns, casts data types using Pandas, and converts it to Parquet format.
4. **Processed Storage**: Saves the cleaned Parquet files into another MinIO bucket (`crypto-processed-data`).
5. **Load (Data Warehouse)**: Reads the Parquet files and loads them into a PostgreSQL database using an Upsert/Append strategy:
   - `dim_coins`: Dimension table storing unique coin information (ID, Name, Symbol).
   - `fact_market_prices`: Fact table storing hourly price, volume, and market cap snapshots.
6. **Orchestration**: Uses Prefect to schedule and monitor the entire pipeline to run automatically every hour.

## 🛠️ Technology Stack

- **Language**: Python 3
- **Data Processing**: Pandas, PyArrow
- **Data Lake (Object Storage)**: MinIO (S3-compatible)
- **Data Warehouse**: PostgreSQL 16
- **Database ORM/Driver**: SQLAlchemy, psycopg2
- **Orchestration**: Prefect
- **Infrastructure**: Docker & Docker Compose
- **Database Management**: pgAdmin4

## 📂 Project Structure

```text
Analyzing-crypto/
├── FetchRawData.py        # Calls API & saves raw JSON to MinIO
├── transform_data.py      # Cleans JSON & saves Parquet to MinIO
├── Loading.py             # Loads Parquet data into PostgreSQL tables
├── Ochestration.py        # Prefect flow connecting the 3 steps above
├── docker-compose.yaml    # Infrastructure config (MinIO, Postgres, pgAdmin)
└── README.md
```

## 🚀 Getting Started

### 1. Prerequisites
- Docker and Docker Compose installed.
- Python 3.9+ installed.

### 2. Start the Infrastructure
Spin up MinIO, PostgreSQL, and pgAdmin using Docker Compose:

```bash
docker-compose up -d
```

**Access the UIs:**
- **MinIO Console**: `http://localhost:9001` (User: `minioadmin`, Pass: `minioadminpassword`)
- **pgAdmin**: `http://localhost:5050` (User: `admin@admin.com`, Pass: `admin`)
  - *To connect pgAdmin to the database, add a new server with Host: `postgres`, Port: `5432`, User: `de_user`, Pass: `de_password`, Database: `crypto_dw`.*

### 3. Install Python Dependencies
Create a virtual environment and install the required packages:

```bash
python -m venv .venv
# Activate on Windows:
.venv\Scripts\activate
# Activate on Mac/Linux:
source .venv/bin/activate

pip install pandas pyarrow requests minio sqlalchemy psycopg2-binary prefect
```

### 4. Run the Pipeline

**Option A: Manual Execution (for testing)**
Run the scripts in order:
```bash
python FetchRawData.py
python transform_data.py
python Loading.py
```

**Option B: Orchestrated Execution (Production)**
Run the Prefect flow which handles everything:
```bash
python Ochestration.py
```
*Note: You can start the Prefect UI to monitor your flows by running `prefect server start` and visiting `http://localhost:4200`.*

## 📊 Analytics Questions to Answer

Once the data is flowing, you can use SQL window functions in PostgreSQL to answer questions like:
- Which coin had the highest growth over 3 consecutive hours? (Using `LAG()` / `LEAD()`)
- What is the moving average of Bitcoin's volume over the last 24 hours? 