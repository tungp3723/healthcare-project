import pendulum
import os

from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# CONFIG
# =========================
PROJECT_ID = "project-8bf7907a-2104-49b0-99f"
LOCATION = "asia-southeast1"
COMPOSER_BUCKET = "asia-southeast1-healthcare--83775f21-bucket"


# =========================
# READ SQL FROM GCS
# =========================
def read_sql_from_gcs(object_name: str) -> str:
    hook = GCSHook()
    return hook.download(
        bucket_name=COMPOSER_BUCKET,
        object_name=object_name,
    ).decode("utf-8")


BRONZE_QUERY = read_sql_from_gcs("data/BQ/bronze.sql")
SILVER_QUERY = read_sql_from_gcs("data/BQ/silver.sql")
GOLD_QUERY = read_sql_from_gcs("data/BQ/gold.sql")


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

    bronze_tables >> silver_tables >> gold_tables
