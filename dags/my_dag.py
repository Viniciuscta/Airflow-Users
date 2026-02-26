from airflow.decorators import dag, task
from datetime import datetime

# importa suas funções
from src.extract import extract_users
from src.transform import transform_users
from src.load_curated import load_users_curated
from src.load_to_s3 import load_users_to_s3


@dag(
    dag_id="users_etl_modular",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "users", "modular"]
)
def users_pipeline():  
    @task
    def extract(ds=None):
        extract_users(ds)
    @task
    def transform(ds=None):
        transform_users(ds)
    @task
    def load(ds=None):
        load_users_curated(ds)
    @task
    def load_s3(ds=None):
        load_users_to_s3(ds)

    extract_task = extract()
    transform_task = transform()
    load_task = load()
    load_s3_task = load_s3()

    extract_task >> transform_task >> load_task >> load_s3_task