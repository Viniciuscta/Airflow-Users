from airflow.decorators import dag, task
from datetime import datetime
import sys
from pathlib import Path

AIRFLOW_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AIRFLOW_HOME / "src"))

# importação das funções
from extract import extract_users
from transform import transform_users
from load_curated import load_users_curated
from load_to_s3 import load_users_to_s3



@dag(
    dag_id = "users_etl_modular",
    start_date = datetime(2024,1,1),
    schedule = "@daily",
    catchup = False,
    tags = ["etl","users","modular"]
)
def users_pipeline():
    @task
    def extract(ds=None):
        extract_users(ds)
    @task
    def transform(ds=None):
        transform_users(ds)
    @task
    def load_curated(ds=None):
        load_users_curated(ds)
    @task
    def load_to_s3(ds=None):
        load_users_to_s3(ds)
    
    extract_task = extract()
    transform_task = transform()
    load_curated_task = load_curated()
    load_to_s3_task = load_to_s3()

    extract_task >> transform_task >> load_curated_task >> load_to_s3_task
