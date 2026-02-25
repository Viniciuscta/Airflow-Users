import logging
from pathlib import Path
import requests
import pandas as pd

logger = logging.getLogger(__name__)

def extract_users(execution_date: str):
    logger.info(f"Iniciando extração de usuários para a data {execution_date}")

    base_path = Path("/opt/airflow/data/raw/users") ### Caminho de pastas do ambiente 
    partition_path = base_path / execution_date # unindo e adicionando o diretório de data de execução
    partition_path.mkdir(parents=True,exist_ok=True)# criando os diretórios se não existirem
    logger.info(f"Diretório RAW garantido em {partition_path}")

    url = "https://jsonplaceholder.typicode.com/users" # URL da API
    logger.info(f"Realizando requisição para {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        logger.info("Requisição concluida com Sucesso")
    except requests.exceptions.RequestException as e:
        logger.error("Falha ao consumir API: %s", e)
        raise  # 🔥 importante: relança o erro

    df = pd.DataFrame(response.json())
    if df.empty:
        raise ValueError("API retornou dataset vazio")
    logger.info(f"API retornou {len(df)} registros") # Criando um Dataframe com a resposta Json da API

    df.to_parquet(partition_path / "users.parquet") # Transformando o dataframe em parquet para que seja entregue através do context para outras funçoes.
    logger.info("Arquivo users.parquet salvo com sucesso na camada RAW")