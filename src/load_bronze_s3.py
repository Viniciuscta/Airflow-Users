import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
logger = logging.getLogger(__name__)


def load_bronze_users_to_s3(execution_date: str):
    logger.info(f"Iniciando upload para S3 | data {execution_date}")
    local_file = (
        f"/opt/airflow/data/bronze/users/{execution_date}/users.parquet"
    )

    bucket_name = "vinicius-airflow-data-lake"
    s3_key = f"bronze/users/{execution_date}/users.parquet"

    logger.info(
        f"Preparando upload do arquivo {local_file} "
        f"para s3://{bucket_name}/{s3_key}"
    )

    s3_hook = S3Hook(aws_conn_id="aws_default")
    logger.info("Conexão com AWS via S3Hook criada com sucesso")

    s3_hook.load_file(
        filename=local_file,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )  
    logger.info("Upload para S3 concluído com sucesso")