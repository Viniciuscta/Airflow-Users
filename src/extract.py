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

    url = "https://randomuser.me/api/" # URL da API
    page = 1
    all_data = []
    while True:
            params = {
        "results": 100, 
        "nat": "us,br"
    }
            response = requests.get(url,params=params, timeout=10)
            response.raise_for_status()
            data = response.json()["data"]

            if not data:
                 logger.info("sem paginas")
                 break
            
            all_data.extend(data)

            logger.info(f"Páginas coletadas{page}")
            page += 1

    df = pd.json_normalize(all_data)
    output_path = partition_path / "users.parquet"
    df.to_parquet(output_path)

    logger.info(f"Extração finalizada com sucesso {len(df)} registros")