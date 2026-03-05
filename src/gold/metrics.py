from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def gold_metrics(execution_date:str):
    input_path = Path(f"/opt/airflow/data/silver/users/{execution_date}/users.parquet")
    output_path = Path(f"/opt/airflow/data/gold/users/{execution_date}")
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Lendo arquivo PROCESSED: {input_path}")

    df = pd.read_parquet(input_path)
    
    metrics = {
    "data_execucao": execution_date,
    "total_usuarios": len(df),
    "media_idade": df["idade"].mean(),
    "idade_minima": df["idade"].min(),
    "idade_maxima": df["idade"].max(),
    "usuarios_masculinos": (df["genero"] == "male").sum(),
    "usuarios_femininos": (df["genero"] == "female").sum(),
    "total_paises": df["pais"].nunique(),
    "total_estados": df["estado"].nunique(),
    "total_cidades": df["cidade"].nunique()
}

    df_gold = pd.DataFrame([metrics])

    output_file = output_path / "users_metrics.parquet"

    df_gold.to_parquet(output_file,index=False)

    return output_file
