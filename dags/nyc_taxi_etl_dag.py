from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 7, 15),
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["nyc_taxi", "spark", "snowflake"],
) as dag:

    run_spark_etl = BashOperator(
        task_id="spark_etl_task",
        bash_command="spark-submit /opt/airflow/scripts/etl_spark.py"
    )
