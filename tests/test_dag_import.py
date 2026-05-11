import py_compile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DAGS_DIR = PROJECT_ROOT / "dags"
DATA_DIR = PROJECT_ROOT / "data"
BQ_DIR = DATA_DIR / "BQ"
INGESTION_DIR = DATA_DIR / "ingestion"


def test_required_dag_files_exist():
    required_files = [
        DAGS_DIR / "parent_dag.py",
        DAGS_DIR / "pyspark_dag.py",
        DAGS_DIR / "bq_dag.py",
    ]

    for file_path in required_files:
        assert file_path.exists(), f"Missing required DAG file: {file_path}"


def test_required_sql_files_exist():
    required_files = [
        BQ_DIR / "bronze.sql",
        BQ_DIR / "silver.sql",
        BQ_DIR / "gold.sql",
    ]

    for file_path in required_files:
        assert file_path.exists(), f"Missing required SQL file: {file_path}"


def test_required_ingestion_files_exist():
    required_files = [
        INGESTION_DIR / "HospitalA_toLanding.py",
        INGESTION_DIR / "HospitalB_toLanding.py",
        INGESTION_DIR / "Claims.py",
        INGESTION_DIR / "CptCode.py",
    ]

    for file_path in required_files:
        assert file_path.exists(), f"Missing required ingestion file: {file_path}"


def test_python_files_compile():
    python_files = [
        DAGS_DIR / "parent_dag.py",
        DAGS_DIR / "pyspark_dag.py",
        DAGS_DIR / "bq_dag.py",
        INGESTION_DIR / "HospitalA_toLanding.py",
        INGESTION_DIR / "HospitalB_toLanding.py",
        INGESTION_DIR / "Claims.py",
        INGESTION_DIR / "CptCode.py",
    ]

    for file_path in python_files:
        py_compile.compile(str(file_path), doraise=True)


def test_parent_dag_contains_expected_config():
    content = (DAGS_DIR / "parent_dag.py").read_text(encoding="utf-8")

    assert "parent_dag" in content
    assert "TriggerDagRunOperator" in content
    assert "pyspark_dag" in content
    assert "bigquery_dag" in content


def test_pyspark_dag_contains_expected_config():
    content = (DAGS_DIR / "pyspark_dag.py").read_text(encoding="utf-8")

    assert "pyspark_dag" in content
    assert "DataprocStartClusterOperator" in content
    assert "DataprocSubmitJobOperator" in content
    assert "DataprocStopClusterOperator" in content
    assert "data/ingestion" in content


def test_bigquery_dag_contains_expected_config():
    content = (DAGS_DIR / "bq_dag.py").read_text(encoding="utf-8")

    assert "bigquery_dag" in content
    assert "BigQueryInsertJobOperator" in content
    assert "bronze.sql" in content
    assert "silver.sql" in content
    assert "gold.sql" in content