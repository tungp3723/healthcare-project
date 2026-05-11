#!/bin/bash
set -e

COMPOSER_ENV="healthcare-airflow"
REGION="asia-southeast1"
DAG_ID="parent_dag"

echo "======================================"
echo "Trigger Airflow DAG"
echo "======================================"

echo "Composer environment: ${COMPOSER_ENV}"
echo "Region: ${REGION}"
echo "DAG ID: ${DAG_ID}"

gcloud composer environments run "${COMPOSER_ENV}" \
  --location "${REGION}" \
  dags trigger -- "${DAG_ID}"

echo "======================================"
echo "DAG triggered successfully"
echo "======================================"