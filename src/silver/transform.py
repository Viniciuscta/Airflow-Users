import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def transform_users(execution_date: str):

    logger.info(f"Iniciando transformação para {execution_date}")

    # -------------------------
    # Paths
    # -------------------------
    raw_file = Path(
        f"/opt/airflow/data/bronze/users/{execution_date}/users.parquet"
    )
    processed_path = Path(
        f"/opt/airflow/data/processed/users/{execution_date}"
    )

    processed_path.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Leitura RAW
    # -------------------------
    logger.info(f"Lendo arquivo RAW: {raw_file}")

    df = pd.read_parquet(raw_file)

    logger.info(f"{len(df)} registros carregados")

    # -------------------------
    # Transformação (flatten JSON)
    # -------------------------
    df_processed = pd.DataFrame({
        "user_id": df["login.uuid"],
        "nome": df["name.first"] + " " + df["name.last"],
        "email": df["email"].str.replace("@", "", regex=False),
        "telefone": df["phone"],
        "celular": df["cell"],
        "genero": df["gender"],
        "idade": df["dob.age"],
        "cidade": df["location.city"],
        "estado": df["location.state"],
        "pais": df["location.country"],
        "data_execucao": execution_date
    })

    # -------------------------
    # Limpeza
    # -------------------------
    df_processed = df_processed.drop_duplicates(subset="user_id")
    df_processed = df_processed.dropna(subset=["user_id", "email"])

    df_processed["nome"] = df_processed["nome"].str.strip()

    if df_processed.empty:
        raise ValueError("Dataset vazio após transformação")

    logger.info(f"Dataset transformado contém {len(df_processed)} registros")

    # -------------------------
    # Escrita PROCESSED
    # -------------------------
    output_file = processed_path / "users.parquet"

    df_processed.to_parquet(
        output_file,
        index=False,
        compression="snappy"
    )

    logger.info(f"Arquivo processado salvo em {output_file}")