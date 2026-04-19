# dag.py

from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pipeline.extractor import DatasetDownloader, DatasetExtractor
from pipeline.consolidator import DataConsolidator
from pipeline.transformer import DataTransformer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STAGING_DIR = Path("/home/sahire/dev/perso/airflow_train/staging")
SOURCE_URL  = (
    "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud"
    "/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
)

# ---------------------------------------------------------------------------
# Callables
# ---------------------------------------------------------------------------
def download_dataset():
    DatasetDownloader(url=SOURCE_URL, dest=STAGING_DIR).download()

def untar_dataset():
    filepath = STAGING_DIR / "tolldata.tgz"
    DatasetExtractor(STAGING_DIR).untar(filepath)

def extract_data_from_csv():
    DatasetExtractor(STAGING_DIR).extract_from_csv()

def extract_data_from_tsv():
    DatasetExtractor(STAGING_DIR).extract_from_tsv()

def extract_data_from_fixed_width():
    DatasetExtractor(STAGING_DIR).extract_from_fixed_width()

def consolidate_data():
    DataConsolidator(STAGING_DIR).consolidate()

def transform_data():
    DataTransformer(STAGING_DIR).transform()

# ---------------------------------------------------------------------------
# DAG arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 18, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
dag = DAG(
    "Python_DAG",
    default_args=default_args,
    description="Apache Airflow ETL pipeline for tolldata",
    schedule=timedelta(days=1),
)

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
t1 = PythonOperator(task_id="download_data",               python_callable=download_dataset,          dag=dag)
t2 = PythonOperator(task_id="unzip_data",                  python_callable=untar_dataset,             dag=dag)
t3 = PythonOperator(task_id="extract_data_from_csv",       python_callable=extract_data_from_csv,     dag=dag)
t4 = PythonOperator(task_id="extract_data_from_tsv",       python_callable=extract_data_from_tsv,     dag=dag)
t5 = PythonOperator(task_id="extract_data_from_fixed_width", python_callable=extract_data_from_fixed_width, dag=dag)
t6 = PythonOperator(task_id="consolidate_data",            python_callable=consolidate_data,          dag=dag)
t7 = PythonOperator(task_id="transform_data",              python_callable=transform_data,            dag=dag)

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7