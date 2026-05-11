import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# CONFIG
# =========================
PROJECT_ID = "project-8bf7907a-2104-49b0-99f"
LOCATION = "asia-southeast1"

# In Cloud Composer, the environment bucket is mounted at /home/airflow/gcs
SQL_BASE_PATH = "/home/airflow/gcs/data/BQ"

default_args = {
    "owner": "TungPD20",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["tungp3723@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

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
                "query": f"{{% include '{SQL_BASE_PATH}/bronze.sql' %}}",
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
                "query": f"{{% include '{SQL_BASE_PATH}/silver.sql' %}}",
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
                "query": f"{{% include '{SQL_BASE_PATH}/gold.sql' %}}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    bronze_tables >> silver_tables >> gold_tables
