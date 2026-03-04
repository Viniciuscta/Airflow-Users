import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def load_users_curated(execution_date: str):  # Função recebendo data de execução da DAG (YYYY-MM-DD)
    logger.info(f"Iniciando carga CURATED para a data {execution_date}")


    processed_file = Path(f"/opt/airflow/data/processed/users/{execution_date}/users.parquet") # Caminho do dado processado (entrada)
    output_path = Path(f"/opt/airflow/data/silver/users/{execution_date}")# Caminho de saída para o CSV final
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Lendo arquivo PROCESSED: {processed_file}")

    df = pd.read_parquet(processed_file) # Leitura do dado processado
    logger.info(f"Dataset processado contém {len(df)} registros")
    
    df.to_csv(output_path / "users.csv", index=False) # Escrita do CSV final
    logger.info("Arquivo CSV final salvo na camada CURATED")



    