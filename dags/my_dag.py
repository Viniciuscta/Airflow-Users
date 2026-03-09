from airflow.decorators import dag, task
from datetime import datetime
from pathlib import Path


# importação das funções
from bronze.extract import extract_users
from load.load_bronze_s3 import load_bronze_users_to_s3
from silver.transform import transform_users
from load.load_silver_s3 import load_silver_users_to_s3
from gold.metrics import gold_metrics
from load.load_gold_s3 import load_gold_users_to_s3
from athena.register_partition import register_partition
from silver.incremental_merge import incremental_merge


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
    def load_bronze_to_s3(ds=None):
        load_bronze_users_to_s3(ds)
    @task
    def transform(ds=None):
        transform_users(ds)
    @task
    def merge(ds=None):
        incremental_merge(ds)
    @task
    def load_silver_to_s3(ds=None):
        load_silver_users_to_s3(ds)
    @task
    def metrics(ds=None):
        gold_metrics(ds)
    @task
    def load_gold_to_s3(ds=None):
        load_gold_users_to_s3(ds)
    @task
    def partition_athenas(ds=None):
        register_partition(ds)

        
    
    extract_task = extract()
    load_bronze_s3 = load_bronze_to_s3()
    transform_task = transform()
    snapshot_task = merge()
    load_silver_s3_task = load_silver_to_s3()
    metrics_task = metrics()
    load_gold_s3 = load_gold_to_s3()
    partition = partition_athenas()

    extract_task >> load_bronze_s3 >> transform_task >> snapshot_task >> load_silver_s3_task >> metrics_task >> load_gold_s3 >> partition


users_pipeline()