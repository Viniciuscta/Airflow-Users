# Airflow ETL Pipeline – Users

Este projeto implementa um pipeline ETL simples utilizando Apache Airflow, com execução diária baseada em `execution_date` e particionamento de dados por data.

O objetivo é demonstrar boas práticas fundamentais de engenharia de dados aplicadas a pipelines batch.

---

## 📌 Objetivo do Projeto

- Automatizar a execução de um pipeline ETL
- Utilizar `execution_date` como eixo central do processamento
- Organizar dados por partição temporal
- Garantir idempotência e possibilidade de reprocessamento

Este projeto foi desenvolvido para fins de estudo e portfólio.

---

## 🏗 Arquitetura de Dados
data/
├── raw/
│ └── users/
│ └── YYYY-MM-DD/
│ └── users.parquet
└── processed/
└── users/
└── YYYY-MM-DD/
└── users.parquet

---

## 🔄 Fluxo do Pipeline

### 1. Extract
- Executa diariamente conforme o schedule da DAG
- Processa dados referentes à `execution_date`
- Persiste os dados brutos particionados por data

### 2. Transform
- Lê os dados da camada raw da mesma `execution_date`
- Aplica transformação simples de enriquecimento
- Persiste os dados na camada processed mantendo o particionamento

---

## ⏱ Agendamento

- Schedule: `@daily`
- Pipeline baseado em período lógico, não no horário real da execução

Esse modelo permite:
- reprocessamento
- backfill
- retries seguros
- rastreabilidade temporal

---

## 🧠 Conceitos Aplicados

- Apache Airflow
- DAGs e PythonOperator
- `execution_date`
- Particionamento por data
- Idempotência
- ETL batch
- Organização de Data Lake (raw / processed)

---

## 🚀 Possíveis Evoluções

- Persistência em S3 ou GCS
- Validações de qualidade de dados
- Versionamento de schema
- Logs estruturados
- Backfill documentado

---

## 📎 Observações

Este repositório contém apenas a lógica da DAG.  
Pressupõe-se a existência de um ambiente Apache Airflow previamente configurado (ex: via Docker).

---