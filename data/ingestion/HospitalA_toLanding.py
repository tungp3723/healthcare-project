# Generated from: HospitalA_toLanding.ipynb
# Converted at: 2026-05-11T16:53:20.312Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json

storage_client = storage.Client()
bq_client = bigquery.Client()

spark = SparkSession.builder.appName('HospitalAToLanding').getOrCreate()

GCS_BUCKET = 'healthcare-tungpd20'
HOSPITAL_NAME = 'hospital-a'
LANDING_PATH = f'gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}'
ARCHIVE_PATH = f'gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/'
CONFIG_FILE_PATH = f'gs://{GCS_BUCKET}/configs/load_config.csv'

BQ_PROJECT = 'project-8bf7907a-2104-49b0-99f'
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"


POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://10.38.64.3:5432/hospital-a",
    "driver": "org.postgresql.Driver",
    "user": "tungpd20",
    "password": "tungp3723"
}

log_entries = []

def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] [{event_type}] - {message}")

# Read Config File
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "Read config file successfully")
    return df

config_df = read_config_file()
config_df.show()

# Logs to GCS
def save_logs_to_gcs():
    """Save logs to a JSON file and upload to GCS"""

    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)

    # Get GCS bucket
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)

    # Upload JSON data as a file
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

from pyspark.sql.types import StructType, StructField, StringType

def save_logs_to_bigquery():
    """Save logs to BigQuery"""

    if log_entries:
        log_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("message", StringType(), True),
            StructField("table", StringType(), True)
        ])

        cleaned_logs = []
        for entry in log_entries:
            cleaned_logs.append({
                "timestamp": "" if entry.get("timestamp") is None else str(entry.get("timestamp")),
                "event_type": "" if entry.get("event_type") is None else str(entry.get("event_type")),
                "message": "" if entry.get("message") is None else str(entry.get("message")),
                "table": "" if entry.get("table") is None else str(entry.get("table"))
            })

        log_df = spark.createDataFrame(cleaned_logs, schema=log_schema)

        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", GCS_BUCKET) \
            .mode("append") \
            .save()

        print("Logs stored in BigQuery for future analysis")

# Function to Move Existing Files to Archive
def move_existing_files_to_archive(table):
    blobs = list(
        storage_client.bucket(GCS_BUCKET).list_blobs(
            prefix=f"landing/{HOSPITAL_NAME}/{table}/"
        )
    )

    existing_files = [
        blob.name for blob in blobs
        if blob.name.endswith(".json")
    ]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return

    run_ts = datetime.datetime.now().strftime("%H%M%S")

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        # Extract Date from File Name
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        filename = file.split("/")[-1]

        # Move to Archive
        archive_path = (
            f"landing/{HOSPITAL_NAME}/archive/{table}/"
            f"{year}/{month}/{day}/{run_ts}/{filename}"
        )

        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # Copy file to archive and delete original
        storage_client.bucket(GCS_BUCKET).copy_blob(
            source_blob,
            storage_client.bucket(GCS_BUCKET),
            destination_blob.name
        )

        source_blob.delete()

        log_event("INFO", f"Moved {file} to {archive_path}", table=table)

# Function to Get Latest Watermark from BigQuery Audit Table
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}'
          AND data_source = 'hospital-a'
    """

    query_job = bq_client.query(query)
    result = query_job.result()

    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"

    return "1900-01-01 00:00:00"

# Function to Extract Data from PostgreSQL and Save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = (
            get_latest_watermark(table)
            if load_type.lower() == "incremental"
            else None
        )

        log_event(
            "INFO",
            f"Latest watermark for {table}: {last_watermark}",
            table=table
        )

        query = (
            f"(SELECT * FROM {table}) AS t"
            if load_type.lower() == "full"
            else f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        )

        df = (
            spark.read.format("jdbc")
            .option("url", POSTGRES_CONFIG["url"])
            .option("user", POSTGRES_CONFIG["user"])
            .option("password", POSTGRES_CONFIG["password"])
            .option("driver", POSTGRES_CONFIG["driver"])
            .option("dbtable", query)
            .load()
        )

        log_event(
            "SUCCESS",
            f"Successfully extracted data from {table}",
            table=table
        )

        today = datetime.datetime.today().strftime("%d%m%Y")
        json_file_path = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(json_file_path)

        blob.upload_from_string(
            df.toPandas().to_json(orient="records", lines=True),
            content_type="application/json"
        )

        log_event(
            "SUCCESS",
            f"JSON file successfully written to gs://{GCS_BUCKET}/{json_file_path}",
            table=table
        )

        # Insert Audit Entry
        audit_df = spark.createDataFrame(
            [
                (
                    "hospital-a",
                    table,
                    load_type,
                    df.count(),
                    datetime.datetime.now(),
                    "SUCCESS"
                )
            ],
            [
                "data_source",
                "tablename",
                "load_type",
                "record_count",
                "load_timestamp",
                "status"
            ]
        )

        (
            audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save()
        )

        log_event(
            "SUCCESS",
            f"Audit log updated for {table}",
            table=table
        )

    except Exception as e:
        log_event(
            "ERROR",
            f"Error processing {table}: {str(e)}",
            table=table
        )

# Processing Data
for row in config_df.collect():
    if row["is_active"] == "1" and row["datasource"] == "hospital-a":

        db = row["database"]
        datasource = row["datasource"]
        table = row["tablename"]
        load_type = row["loadtype"]
        watermark = row["watermark"]
        targetpath = row["targetpath"]

        move_existing_files_to_archive(table)

        extract_and_save_to_landing(
            table,
            load_type,
            watermark
        )

save_logs_to_gcs()
save_logs_to_bigquery()