# scripts/etl_spark.py


from dotenv import load_dotenv
import os
import glob
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import snowflake.connector

# Load .env variables
load_dotenv()

# 1️⃣ Start Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi ETL - Parquet") \
    .master("local[*]") \
    .getOrCreate()

# 2️⃣ Read raw Parquet data
print("🚦 Reading raw input file ...")
df_raw = spark.read.parquet("data/raw/yellow_tripdata_2024-12.parquet")

# 3️⃣ Validate raw data
raw_count = df_raw.count()
print(f"✅ RAW DF COUNT: {raw_count}")
df_raw.show(5)

if raw_count == 0:
    print("🚫 RAW input is empty! Check your source file path.")
    spark.stop()
    exit()

# 4️⃣ Filter: basic transform
df_clean = df_raw.filter(
    (col("passenger_count") > 0) &
    (col("fare_amount") > 0)
)

clean_count = df_clean.count()
print(f"✅ CLEAN DF COUNT: {clean_count}")
df_clean.show(5)

if clean_count == 0:
    print("🚫 Filtered DataFrame is empty! Adjust your filter conditions.")
    spark.stop()
    exit()

# 🟢 NEW: Force Spark to fully materialize the dataset into one partition
df_clean = df_clean.coalesce(1).persist()

# 5️⃣ Write to a safe, local folder outside OneDrive
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_folder = f"C:/temp/nyc_taxi_etl/yellow_tripdata_cleaned_{timestamp}.parquet"

os.makedirs(os.path.dirname(output_folder), exist_ok=True)

print(f"💾 Writing cleaned data to: {output_folder}")
df_clean.write.mode("overwrite").parquet(output_folder)
print("✅ Write completed.")

# 6️⃣ Find the part file and verify size
import time
time.sleep(2)  # small pause to ensure filesystem updates

files = glob.glob(f"{output_folder}/part-*.parquet")
if not files:
    print("🚫 No part files found. Spark write may have failed.")
    spark.stop()
    exit()

absolute_file_path = os.path.abspath(files[0])
absolute_file_path_posix = Path(absolute_file_path).as_posix()
file_size_bytes = os.path.getsize(absolute_file_path)

print(f"✅ Found part file: {absolute_file_path}")
print(f"✅ POSIX path for PUT: {absolute_file_path_posix}")
print(f"✅ File size: {file_size_bytes / 1024:.2f} KB")

if file_size_bytes < 50 * 1024:  # ~50 KB minimum
    print("🚫 File too small — likely empty or filter too aggressive. Aborting.")
    spark.stop()
    exit()

# 7️⃣ Connect to Snowflake
print("🔗 Connecting to Snowflake ...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cs = conn.cursor()

# 8️⃣ Remove old files from stage to be safe
print("🗑️ Removing old files from @my_stage ...")
cs.execute("REMOVE @my_stage;")

# 9️⃣ PUT the new file to stage using POSIX path
put_command = f"PUT 'file://{absolute_file_path_posix}' @my_stage AUTO_COMPRESS=FALSE"
print(f"📤 PUT command: {put_command}")

cs.execute(put_command)
print("✅ File PUT to @my_stage!")

# 🔟 Confirm file in stage
cs.execute("LIST @my_stage;")
print("✅ Staged files:", cs.fetchall())

# 1️⃣1️⃣ Run COPY INTO with correct mapping
copy_command = """
COPY INTO nyc_taxi_cleaned
FROM @my_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FORCE = TRUE;
"""
print("🚦 Running COPY INTO ...")
cs.execute(copy_command)
print("✅ COPY INTO completed!")

# 1️⃣2️⃣ Final row count in Snowflake
cs.execute("SELECT COUNT(*) FROM nyc_taxi_cleaned;")
row_count = cs.fetchone()[0]
print(f"✅ Final rows in nyc_taxi_cleaned: {row_count}")

# Cleanup
cs.close()
conn.close()
spark.stop()
print("🏁 Pipeline complete — all done!")
