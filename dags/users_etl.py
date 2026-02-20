from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

def extract_users(**context):
    execution_date = context["ds"] # YYYY-MM-DD

    base_path = Path("/opt/airflow/data/raw/users")
    partition_path = base_path / execution_date
    partition_path.mkdir(parents=True,exist_ok=True)

    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    df.to_parquet(partition_path / "users.parquet")

def transform_users(**context):
    execution_date = context["ds"]

    raw_file = Path(f"/opt/airflow/data/raw/users/{execution_date}/users.parquet")
    processed_path = Path(f"/opt/airflow/data/processed/users/{execution_date}")
    processed_path.mkdir(parents=True,exist_ok=True)

    df = pd.read_parquet(raw_file)
    df["email_domain"] = df["email"].str.split("@").str[-1]
    df.to_parquet(processed_path / "users.parquet")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id = "users_etl_daily",
    start_date = datetime(2024,1,1),
    schedule="@daily",
    catchup = False,
    default_args=default_args,
    tags = ["automation","etl","partitioned"],
) as dag:

    extract = PythonOperator(
        task_id = "extract_users",
        python_callable = extract_users,
    )

    transform = PythonOperator(
        task_id = "transform_users",
        python_callable = transform_users,
    )

    extract >> transform