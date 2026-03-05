The pipeline is orchestrated through **Apache Airflow DAGs**, which manage task execution, dependencies, retries, and scheduling.

![ETL Pipeline Architecture](docs/images/pipeline_image.png)

---

# 🥇 Medallion Data Architecture

This pipeline follows the **Medallion Architecture**, a widely used design pattern in modern Data Lakes.

| Layer | Purpose |
|------|------|
| Bronze | Raw data ingestion directly from the source API |
| Silver | Cleaned, normalized, and structured dataset |
| Gold | Aggregated metrics and analytical datasets |

Benefits of this architecture:

- Clear **data lineage**
- Separation between raw and curated data
- Improved **data quality management**
- Scalable analytics-ready datasets
- Easier debugging and pipeline maintenance

---

# 📂 Project Structure

```
AIRFLOW-PROJECT/
│
├── dags/ # Airflow DAG definitions
│
├── docs/
│ └── images/ # Architecture diagrams & visuals
│
├── src/
│ ├── bronze/ # Raw ingestion layer
│ │ └── extract_users.py
│ │
│ ├── silver/ # Data cleaning & transformation
│ │ └── transform_users.py
│ │
│ ├── gold/ # Analytical metrics generation
│ │ └── gold_metrics.py
│ │
│ ├── load/ # S3 upload logic
│ │ ├── load_bronze_s3.py
│ │ ├── load_silver_s3.py
│ │ └── load_gold_s3.py
│ │
│ └── utils/ # Shared utilities
│
├── tests/
│ └── test_transform.py # Unit tests for transformations
│
├── docker-compose.yaml # Airflow local environment
├── requirements.txt # Project dependencies
└── README.md
```

---

# ⚙️ Pipeline Design Principles

The pipeline was built following **modern Data Engineering best practices**.

---

## 1️⃣ Modular Architecture

Each pipeline stage is implemented as an independent module.

| Layer | Responsibility |
|------|------|
| Extract | Data ingestion from external source |
| Transform | Data cleaning and structuring |
| Gold | Analytical metrics generation |
| Load | Persist datasets in the Data Lake |
| Orchestration | Task scheduling and dependencies |

Benefits:

- Easier maintenance
- Testable components
- Clear separation of concerns
- Scalable pipeline design

---

## 2️⃣ Incremental Processing

The pipeline processes data based on the **execution date**, allowing each run to generate isolated datasets.

This simulates a real-world **incremental ingestion strategy** commonly used in production pipelines.

Benefits:

- Reduced processing overhead
- Faster pipeline execution
- Historical data versioning

---

## 3️⃣ Idempotent Pipeline Design

Pipeline tasks are designed to be **safe to re-run**.

If a task executes multiple times:

- No duplicate data is created
- Outputs remain consistent
- The pipeline can recover safely from failures

This behavior is critical for reliable production pipelines.

---

## 4️⃣ Logging & Observability

The pipeline includes structured logging to support:

- Execution monitoring
- Debugging failures
- Tracking processed records

Logs are integrated with **Airflow task monitoring**.

---

## 5️⃣ Error Handling

Defensive programming techniques are applied to ensure pipeline reliability:

- Controlled exceptions
- Retry-safe tasks
- Clear logging for failure diagnosis

---

# 📊 Data Source

The pipeline ingests synthetic user data from the **RandomUser API**, commonly used for testing data pipelines.

Example fields included in the dataset:

- User ID
- Name
- Email
- Gender
- Age
- Phone and mobile numbers
- City
- State
- Country

This dataset simulates realistic ingestion and transformation scenarios.

---

# ☁️ Data Lake Storage (Amazon S3)

Processed datasets are stored in **Amazon S3**, simulating a modern **Data Lake architecture**.

The storage structure follows the Medallion pattern:
```
s3://data-lake/

bronze/
users/

silver/
users/

gold/
users_metrics/
```


All datasets are stored in **Parquet format**, providing:

- Columnar storage
- Better compression
- Faster analytical queries
- Compatibility with engines such as Athena or Spark

---

# 📦 Data Partitioning Strategy

Datasets are partitioned by **execution date**, following a common Data Lake optimization strategy.

Example structure:

```
data/

bronze/users/2026-03-05/users.parquet
silver/users/2026-03-05/users.parquet
gold/users/2026-03-05/users_metrics.parquet
```


Partitioning improves:

- Query performance
- Data organization
- Incremental processing
- Scalability for large datasets

This approach mirrors how enterprise data platforms manage **time-based datasets**.

---

# ⏰ Orchestration (Apache Airflow)

The pipeline is orchestrated using **Apache Airflow**.

Features used:

- DAG scheduling
- Task dependency management
- Execution history tracking
- Retry mechanisms
- Task-level logging

---

# 🧪 Testing

Basic **unit tests** validate transformation logic.

## 2️⃣ Start Airflow Environment

```bash
docker compose up -d
```

---

## 3️⃣ Access Airflow UI

```
http://localhost:8080
```

Credentials are defined in the Docker configuration.

---

# ▶️ Execute the Pipeline

1. Open the Airflow UI  
2. Enable the DAG  
3. Trigger a run or wait for the scheduled execution  
4. Monitor task logs and execution progress  

---

# 📈 Engineering Concepts Demonstrated

This project demonstrates several real-world **Data Engineering concepts**:

- Apache Airflow orchestration  
- Medallion Data Architecture  
- Incremental ingestion  
- Data Lake design  
- Parquet-based storage  
- Modular pipeline architecture  
- Logging and observability  
- Idempotent pipeline design  
- Dockerized development environment  

---

# 📬 Future Improvements

Potential improvements for the pipeline include:

- Data quality validation layer  
- AWS Athena integration for querying  
- AWS Glue Data Catalog integration  
- Metadata tracking  
- Configuration management  
- CI/CD pipeline for DAG validation  
- Infrastructure as Code (IaC)  

---

# 👨‍💻 Author

**Vinicius Santos**

Data Engineering Enthusiast focused on building **production-oriented data pipelines** and **scalable data architectures**.
