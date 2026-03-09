import logging
from airflow.providers.amazon.aws.hooks.athena import AthenaHook

logger = logging.getLogger(__name__)

def register_partition(execution_date: str):

    logger.info(f"Registrando partição para data {execution_date}")

    hook = AthenaHook(
        aws_conn_id="aws_default",
        region_name="us-east-1"
    )

    athena = hook.get_conn()

    query = f"""
    ALTER TABLE users
    ADD IF NOT EXISTS PARTITION (date='{execution_date}')
    LOCATION 's3://vinicius-airflow-data-lake/gold/users/date={execution_date}/'
    """

    logger.info(f"Executando query no Athena: {query}")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": "db_data_lake"
        },
        ResultConfiguration={
            "OutputLocation": "s3://vinicius-airflow-data-lake/athena-results/"
        },
        WorkGroup="primary"
    )

    query_execution_id = response["QueryExecutionId"]

    logger.info(f"Query enviada com sucesso. Execution ID: {query_execution_id}")