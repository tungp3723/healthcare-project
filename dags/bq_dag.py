import os
import pendulum

from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# CONFIG
# =========================
PROJECT_ID = "project-8bf7907a-2104-49b0-99f"
LOCATION = "asia-southeast1"

# Lấy thư mục hiện tại của file DAG
DAG_DIR = os.path.dirname(os.path.abspath(__file__))

# SQL files nằm trong thư mục: dags/data/BQ/
SQL_BASE_PATH = os.path.join(DAG_DIR, "data", "BQ")

SQL_FILE_PATH_1 = os.path.join(SQL_BASE_PATH, "bronze.sql")
SQL_FILE_PATH_2 = os.path.join(SQL_BASE_PATH, "silver.sql")
SQL_FILE_PATH_3 = os.path.join(SQL_BASE_PATH, "gold.sql")


# =========================
# READ SQL FILE
# =========================
def read_sql_file(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)


# =========================
# DEFAULT ARGS
# =========================
default_args = {
    "owner": "TungPD20",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["tungp3723@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}


# =========================
# DAG
# =========================
with DAG(
    dag_id="bigquery_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    description="DAG to run BigQuery jobs",
    default_args=default_args,
    tags=["gcs", "bq", "etl", "elt"],
) as dag:

    # Task 1: Create bronze tables
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task 2: Create silver tables
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task 3: Create gold tables
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task dependencies
    bronze_tables >> silver_tables >> gold_tables
