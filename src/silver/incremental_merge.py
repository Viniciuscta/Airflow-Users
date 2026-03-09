import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

MASTER_PATH = Path("/opt/airflow/data/silver/users_master/users.parquet")


def incremental_merge(execution_date: str):

    logger.info(f"Iniciando merge incremental para {execution_date}")

    # -------------------------
    # Snapshot do dia
    # -------------------------
    today_file = Path(
        f"/opt/airflow/data/silver/users/{execution_date}/users.parquet"
    )

    df_today = pd.read_parquet(today_file)

    logger.info(f"Registros do dia: {len(df_today)}")

    # -------------------------
    # Dataset acumulado
    # -------------------------
    if MASTER_PATH.exists():

        df_existing = pd.read_parquet(MASTER_PATH)
        logger.info(f"Registros acumulados: {len(df_existing)}")

    else:

        logger.info("Primeira execução - criando dataset acumulado")
        df_existing = pd.DataFrame()

    # -------------------------
    # Detecta novos usuários
    # -------------------------
    if df_existing.empty:

        df_new = df_today

    else:

        df_new = df_today[
            ~df_today["user_id"].isin(df_existing["user_id"])
        ]

    logger.info(f"Novos usuários detectados: {len(df_new)}")

    # -------------------------
    # Merge
    # -------------------------
    df_final = pd.concat([df_existing, df_new], ignore_index=True)

    MASTER_PATH.parent.mkdir(parents=True, exist_ok=True)

    df_final.to_parquet(
        MASTER_PATH,
        index=False,
        compression="snappy"
    )

    logger.info(f"Dataset acumulado atualizado: {len(df_final)} registros")