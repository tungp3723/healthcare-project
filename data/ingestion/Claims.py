# Generated from: Claims.ipynb
# Converted at: 2026-05-11T16:52:59.650Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

BUCKET_NAME = "healthcare-tungpd20"

# Read all CSV files inside landing/claims/
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"

BQ_PROJECT = "project-8bf7907a-2104-49b0-99f"
BQ_TABLE = f"{BQ_PROJECT}.bronze_dataset.claims"

TEMP_GCS_BUCKET = f"gs://{BUCKET_NAME}/temp/"

claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header = True)

claims_df = (claims_df
                .withColumn("datasource", 
                              when(input_file_name().contains("hospital2"), "hosb")
                             .when(input_file_name().contains("hospital1"), "hosa").otherwise("None")))

claims_df.printSchema()

claims_df = claims_df.dropDuplicates()

(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())