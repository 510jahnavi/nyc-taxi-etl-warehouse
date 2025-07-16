```markdown
# NYC Taxi ETL Pipeline 🚕✨

This project is an end-to-end **ETL pipeline** for NYC Taxi trip data:
- Ingests Parquet data
- Cleans and filters rows using **Apache Spark**
- Loads cleaned data into a **Snowflake** data warehouse
- Orchestrated by **Apache Airflow**, running in **Docker**

---

## 🚀 Tech Stack

- **Apache Spark**: Data cleaning & transformation
- **Snowflake**: Cloud data warehouse
- **Apache Airflow**: Workflow orchestration
- **Docker Compose**: Local dev & deployment

---

## 📂 Project Structure

```

.
├── dags/
│   └── nyc\_taxi\_etl\_dag.py     # Airflow DAG calling spark-submit
├── scripts/
│   └── etl\_spark.py            # Standalone Spark ETL
├── data/
│   ├── raw/                    # Input Parquet files
│   └── processed/              # Cleaned Parquet output
├── airflow/                    # Airflow metadata DB & logs (local only)
├── docker-compose.yml          # Airflow setup in Docker
├── .env                        # ❗ NOT committed — contains Snowflake creds
└── README.md

```

---

## ⚙️ How It Works

1. **Spark ETL**
   - Reads raw NYC Yellow Taxi data (Parquet)
   - Filters out invalid rows (e.g., negative fare/passenger count)
   - Writes cleaned data as single-part Parquet
   - Uses `.env` for Snowflake creds

2. **Load to Snowflake**
   - Uses `PUT` and `COPY INTO` to stage & load cleaned data
   - Loads into `nyc_taxi_cleaned` table
   - Validates row counts

3. **Airflow Orchestration**
   - `nyc_taxi_etl_dag` uses `BashOperator`:
     - Runs Spark job with `spark-submit`
   - Uses environment variables from `.env`
   - All containers run via Docker Compose

---

## 🗝️ Environment Variables

Create a `.env` in your project root:
```

SNOWFLAKE\_USER=your\_username
SNOWFLAKE\_PASSWORD=your\_password
SNOWFLAKE\_ACCOUNT=your\_account
SNOWFLAKE\_WAREHOUSE=your\_warehouse
SNOWFLAKE\_DATABASE=your\_database
SNOWFLAKE\_SCHEMA=your\_schema

````
❗ **Never commit this file!** Add `.env` to `.gitignore`.

---

## 🐳 Quick Start

```bash
# 1️⃣ Clone this repo
git clone https://github.com/YOUR_GITHUB_USERNAME/nyc-taxi-etl.git
cd nyc-taxi-etl

# 2️⃣ Create .env file (see above)

# 3️⃣ Initialize Airflow DB & admin user
docker compose up airflow-init

# 4️⃣ Start Airflow webserver & scheduler
docker compose up -d

# 5️⃣ Access Airflow UI:
http://localhost:8080
username: admin
password: admin

# 6️⃣ Trigger the DAG & watch logs!
````

---

## ✅ Next Improvements

* [ ] Use Airflow Variables & Connections for secrets
* [ ] Store raw files in S3 and use external stage
* [ ] Add Slack/Email failure notifications
* [ ] Schedule DAG to run daily
* [ ] Deploy Spark on a real cluster

---

## ✨ Credits

Created as a practical data engineering project to demonstrate ETL orchestration, big data processing, and cloud data warehousing.

---

**📬 Questions? PRs welcome!**

````

---

## ✅✅✅ HOW TO PUSH TO GITHUB

1️⃣ **Initialize git** (if you haven’t):
```bash
git init
````

2️⃣ **Add remote** (if you haven’t):

```bash
git remote add origin https://github.com/YOUR_USERNAME/nyc-taxi-etl.git
```

3️⃣ **Stage everything**:

```bash
git add .
```

4️⃣ **Double check your `.gitignore` includes `.env`!**

```bash
# Always test:
git status
```

You should NOT see `.env` as staged.

5️⃣ **Commit your changes**:

```bash
git commit -m "Initial commit: NYC Taxi ETL with Spark, Airflow, Snowflake, Docker"
```

6️⃣ **Push to GitHub**:

```bash
git branch -M main  # optional
git push -u origin main
```


