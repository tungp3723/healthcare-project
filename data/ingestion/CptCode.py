# Generated from: CptCode.ipynb
# Converted at: 2026-05-11T16:53:11.790Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

from pyspark.sql import SparkSession

BUCKET_NAME = "healthcare-tungpd20"

CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"

BQ_PROJECT = "project-8bf7907a-2104-49b0-99f"
BQ_DATASET = "bronze_dataset"
BQ_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.cpt_codes"

TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"


cptcodes_df = spark.read.csv(
    CPT_BUCKET_PATH,
    header=True
)


cptcodes_df.printSchema()

# replace spaces with underscore
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# write to bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())