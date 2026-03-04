from airflow.decorators import dag, task
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, '/opt/airflow/src')

# importação das funções
from bronze.extract import extract_users
from bronze.load_bronze_s3 import load_bronze_users_to_s3
from silver.transform import transform_users
from silver.load_silver import load_users_curated
from silver.load_silver_s3 import load_silver_users_to_s3
from dotenv import load_dotenv


env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

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
    def load_silver_to_s3(ds=None):
        load_silver_users_to_s3(ds)
    @task
    def load_bronze_to_s3(ds=None):
        load_bronze_users_to_s3(ds)
    
    extract_task = extract()
    load_bronze_s3 = load_bronze_to_s3
    transform_task = transform()
    load_curated_task = load_curated()
    load_silver_s3_task = load_silver_to_s3()

    extract_task >> load_bronze_s3 >> transform_task >> load_curated_task >> load_silver_s3_task

users_pipeline()