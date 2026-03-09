import logging
from pathlib import Path
import requests
import pandas as pd

logger = logging.getLogger(__name__)

def extract_users(execution_date: str):

    logger.info(f"Iniciando extração para {execution_date}")

    # caminho particionado por data
    base_path = Path(f"/opt/airflow/data/bronze/users")
    partition_path = base_path / execution_date
    partition_path.mkdir(parents=True, exist_ok=True)

    url = "https://randomuser.me/api/"

    params = {
        "results": 100,
        "nat": "us,br",
        "seed": execution_date  # 🔥 chave mágica
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na API: {e}")
        raise

    data = response.json()["results"]

    if not data:
        raise ValueError("API retornou vazio")

    df = pd.json_normalize(data)

    output_path = partition_path / "users.parquet"
    df.to_parquet(output_path, index=False)

    logger.info(f"{len(df)} usuários salvos em {output_path}")