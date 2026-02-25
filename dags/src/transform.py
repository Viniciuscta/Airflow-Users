import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def transform_users(execution_date: str):#Pegando informações de data de execução através do dia que as tasks do context foram executadas
    

    raw_file = Path(f"/opt/airflow/data/raw/users/{execution_date}/users.parquet") #Diretório Base Bruto(RAW) onde os arquivos executados serão armazenados
    processed_path = Path(f"/opt/airflow/data/processed/users/{execution_date}") #Diretório Base Processado(Limpo) onde os arquivos serão armazenados
    processed_path.mkdir(parents=True,exist_ok=True) #Cria a estrutura de diretórios necessária para salvar os dados processados, evitando erro caso o caminho já exista
    logger.info(f"Lendo arquivo RAW: {raw_file}")

    if not raw_file.exists():
        raise FileNotFoundError(f"Arquivo Não encontrado: {raw_file}")

    df = pd.read_parquet(raw_file) #Lê os dados brutos gerados na etapa de extração
    logger.info(f"Dataset RAW carregado com {len(df)} linhas")

    if 'email' not in df.columns: 
        raise ValueError("Coluna email não existe no dataset")

    df["email_domain"] = df["email"].str.split("@").str[-1] #Trata de forma simples a coluna email, retirando um caractere especial
    logger.info("Coluna email_domain criada com sucesso")

    df.to_parquet(processed_path / "users.parquet") #Transforma novamente em parquet para que seja entregue através do context para outras funções.
    logger.info("Arquivo processado salvo na camada PROCESSED")