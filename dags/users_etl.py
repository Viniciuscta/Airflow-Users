from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path #Biblioteca para lidar com caminhos de forma simples
import requests
import pandas as pd

def extract_users(**context):
    execution_date = context["ds"] # YYYY-MM-DD

    base_path = Path("/opt/airflow/data/raw/users") ### Caminho de pastas do ambiente 
    partition_path = base_path / execution_date # unindo e adicionando o diretório de data de execução
    partition_path.mkdir(parents=True,exist_ok=True)# criando os diretórios se não existirem

    url = "https://jsonplaceholder.typicode.com/users" # URL da API
    response = requests.get(url, timeout=10) # Pegando a requisição da API
    response.raise_for_status() # Esperando por status 200 

    df = pd.DataFrame(response.json()) # Criando um Dataframe com a resposta Json da API
    df.to_parquet(partition_path / "users.parquet") # Transformando o dataframe em parquet para que seja entregue através do context para outras funçoes.

def transform_users(**context):
    execution_date = context["ds"]#Pegando informações de data de execução através do dia que as tasks do context foram executadas
    raw_file = Path(f"/opt/airflow/data/raw/users/{execution_date}/users.parquet") #Diretório Base Bruto(RAW) onde os arquivos executados serão armazenados
    processed_path = Path(f"/opt/airflow/data/processed/users/{execution_date}") #Diretório Base Processado(Limpo) onde os arquivos serão armazenados
    processed_path.mkdir(parents=True,exist_ok=True) #Cria a estrutura de diretórios necessária para salvar os dados processados, evitando erro caso o caminho já exista


    df = pd.read_parquet(raw_file) #Lê os dados brutos gerados na etapa de extração
    df["email_domain"] = df["email"].str.split("@").str[-1] #Trata de forma simples a coluna email, retirando um caractere especial
    df.to_parquet(processed_path / "users.parquet") #Transforma novamente em parquet para que seja entregue através do context para outras funções.


def salvar_csv(**context):
    execution_date = context["ds"]  # Data de execução da DAG (YYYY-MM-DD)

    # Caminho do dado processado (entrada)
    processed_file = Path(f"/opt/airflow/data/processed/users/{execution_date}/users.parquet")

    # Caminho de saída para o CSV final
    output_path = Path(f"/opt/airflow/data/final/users/{execution_date}")
    output_path.mkdir(parents=True, exist_ok=True)

    # Leitura do dado processado
    df = pd.read_parquet(processed_file)

    # Escrita do CSV final
    df.to_csv(output_path / "users.csv", index=False)


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
} #define configurações padrão que serão aplicadas ás tasks da DAG
    

with DAG(
    dag_id = "portifolio_project_users", #ID unico da DAG no airflow (nome da DAG)
    start_date = datetime(2024,1,1), #data inicial a partir de qual dag pode ser executada
    schedule="@daily", # frequencia na qual a dag será executada
    catchup = False, # Evita execução retroativa de datas já passadas   
    default_args=default_args, # Parâmetros padrão aplicados às tasks da DAG
    tags = ["automation","etl","partitioned"], #tags para organização e filtragem no airflow
) as dag:

    extract = PythonOperator(
        task_id = "extract_users", #ID unico da task de extração 
        python_callable = extract_users, #chamando a função responsavel por extrair os dados
    )

    transform = PythonOperator(
        task_id = "transform_users", #ID unico da task de extração 
        python_callable = transform_users, #chamando a função responsavel por transformar os dados
    )
    load = PythonOperator(
        task_id = "load_users", #ID unico da task de extração 
        python_callable = salvar_csv, #chamando a função responsavel por carregar os dados
    )

    extract >> transform >> load # Define a ordem de execução das tasks (extract → transform → load)
