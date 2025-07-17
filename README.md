# NYC Taxi ETL Pipeline 🚕✨

A complete end-to-end ETL pipeline using **Apache Airflow**, **PySpark**, and **Snowflake**, containerized with **Docker Compose**.

---
## 🚀 Overview

- **Input**: Raw NYC Yellow Taxi `.parquet` data
- **ETL**: Data cleaning with PySpark
- **Orchestration**: DAGs using Apache Airflow
- **Warehouse**: Final data loaded into Snowflake
- **Containerization**: All services run in Docker

---

## 📂 Project Structure
```
.
├── dags/                  # Airflow DAGs
│   └── nyc_taxi_etl_dag.py
├── scripts/               # Standalone Spark ETL script
│   └── etl_spark.py
├── data/
│   ├── raw/               # Raw Parquet files
│   └── processed/         # Cleaned Parquet output
├── airflow/               # Airflow metadata DB & logs (local only)
├── docker-compose.yml     # Docker Compose config
├── .env                   # ❗ NOT committed — contains Snowflake creds
└── README.md
```

---

## ⚙️ Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/your-username/nyc-taxi-etl-warehouse.git
cd nyc-taxi-etl-warehouse
```

### 2. Create `.env` file for Snowflake credentials
```bash
touch .env
```

Paste this inside `.env` (replace with your credentials):
```env
SNOWFLAKE_USER=MISJAHNAVI510
SNOWFLAKE_PASSWORD=YourPassword
SNOWFLAKE_ACCOUNT=DHXZDRX-ZI36614
SNOWFLAKE_WAREHOUSE=MY_WH
SNOWFLAKE_DATABASE=NYC_TAXI_DB
SNOWFLAKE_SCHEMA=NYC_TAXI_SCHEMA
```

> ✅ `.env` is **ignored** by Git via `.gitignore`

---

### 3. Add Raw Data

Place at least one `.parquet` file in:
```
data/raw/yellow_tripdata_2024-12.parquet
```

You can download sample data from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

### 4. Start the Airflow stack
```bash
docker compose up --build
```

> Access Airflow at: http://localhost:8080  
> Default login: `admin / admin`

---

### 5. Trigger the DAG

- Open the **Airflow UI**
- Toggle the `nyc_taxi_etl_dag` to **"On"**
- Click ▶️ to trigger the DAG manually

---

## 🧠 DAG Flow

1. **PySpark ETL**:  
   - Reads `.parquet` files from `data/raw/`
   - Cleans data: removes rows with 0 passengers or fare
   - Writes cleaned data to `data/processed/` as `.parquet`

2. **Snowflake Load**:  
   - Reads latest `.parquet` file
   - Uploads to Snowflake stage
   - Loads into `nyc_taxi_cleaned` table via `COPY INTO`

---

## 🐍 Run Spark script locally (optional)
```bash
spark-submit scripts/etl_spark.py
```

---

## 🧾 Sample Output

Logs show counts:
```
✅ RAW DF COUNT: 6131
✅ CLEAN DF COUNT: 5678
✅ Final rows in nyc_taxi_cleaned: 5678
```

---

## 🧹 To stop & clean up
```bash
docker compose down --volumes
```

---

## 🔒 Security

- Snowflake credentials are stored in `.env` file (never committed).
- Output `.parquet` files are verified for size (>50KB) before uploading.
- Errors raise exceptions and prevent partial uploads.

---

## 🔁 Pushing to GitHub

```bash
git add .
git commit -m "Initial ETL pipeline with Airflow, Spark & Snowflake"
git remote add origin https://github.com/your-username/nyc-taxi-etl-warehouse.git
git push -u origin main
```

---

## 📜 License

MIT © 2025
