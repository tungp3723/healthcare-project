#!/bin/bash
set -e

echo "======================================"
echo "Start validation"
echo "======================================"

echo "Checking DAG Python syntax..."
python -m py_compile dags/*.py

echo "Checking ingestion Python syntax..."
python -m py_compile data/ingestion/*.py

echo "Checking required SQL files..."
test -f data/BQ/bronze.sql
test -f data/BQ/silver.sql
test -f data/BQ/gold.sql

echo "Checking required ingestion files..."
test -f data/ingestion/HospitalA_toLanding.py
test -f data/ingestion/HospitalB_toLanding.py
test -f data/ingestion/Claims.py
test -f data/ingestion/CptCode.py

echo "Running pytest..."
pytest tests/ -v

echo "======================================"
echo "Validation passed"
echo "======================================"