#!/bin/bash
set -e

COMPOSER_BUCKET="asia-southeast1-healthcare--ea0d5755-bucket"

echo "======================================"
echo "Start deployment"
echo "======================================"

echo "Using Composer bucket: gs://${COMPOSER_BUCKET}"

echo "Deploying DAG files..."
gsutil -m rsync -r dags "gs://${COMPOSER_BUCKET}/dags"

echo "Deploying data files..."
gsutil -m rsync -r data "gs://${COMPOSER_BUCKET}/data"

echo "Listing deployed DAG files..."
gsutil ls "gs://${COMPOSER_BUCKET}/dags/"

echo "Listing deployed BQ SQL files..."
gsutil ls "gs://${COMPOSER_BUCKET}/data/BQ/"

echo "Listing deployed ingestion files..."
gsutil ls "gs://${COMPOSER_BUCKET}/data/ingestion/"

echo "======================================"
echo "Deployment completed"
echo "======================================"