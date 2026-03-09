import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
logger = logging.getLogger(__name__)


def load_silver_users_to_s3(execution_date: str):
   
    bucket_name = "vinicius-airflow-data-lake"

    s3_hook = S3Hook(aws_conn_id="aws_default")
    uploads = [
        {
            "local": f"/opt/airflow/data/silver/users/{execution_date}/users.parquet",
            "s3": f"silver/users/{execution_date}/users.parquet"
        },
        {
            "local": "/opt/airflow/data/silver/users_master/users.parquet",
            "s3": "silver/users_master/users.parquet"
        }
    ]
    for file in uploads:

        logger.info(
            f"Uploading {file['local']} -> s3://{bucket_name}/{file['s3']}"
        )

        s3_hook.load_file(
            filename=file["local"],
            key=file["s3"],
            bucket_name=bucket_name,
            replace=True
        )

