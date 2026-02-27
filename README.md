# 🚀 Airflow ETL Pipeline — Incremental Data Pipeline with AWS S3

## 📌 Overview

This project implements a **production-style ETL pipeline** orchestrated with **Apache Airflow**, designed to simulate a real-world data engineering workflow.

The pipeline extracts data, applies transformations, and loads curated datasets into **Amazon S3**, following modern data engineering best practices such as:

* Modular architecture
* Incremental processing
* Idempotent executions
* Structured logging
* Error handling
* Daily orchestration scheduling

The goal of this project is to demonstrate how a Data Engineer designs reliable and maintainable pipelines rather than simple scripts.

---

## 🏗️ Architecture

```
Source Data
     ↓
Extract Layer
     ↓
Transform Layer
     ↓
Curated Dataset
     ↓
Amazon S3 (Data Lake)
```

The following diagram represents the ETL workflow orchestrated by Airflow:

![ETL Pipeline Architecture](docs/images/pipeline_image.png)

### Key Characteristics

* ✅ Daily scheduled pipeline (Airflow DAG)
* ✅ Incremental ingestion strategy
* ✅ Idempotent execution (safe re-runs)
* ✅ Centralized logging
* ✅ Error handling and retry-safe tasks
* ✅ Clean modular structure
* ✅ Testable transformation layer

---

## 📂 Project Structure

```
AIRFLOW-PROJECT/
│
├── dags/                  # Airflow DAG definitions
│
├── docs/
│   └── images/            # Architecture & logging visuals
│
├── src/
│   ├── extract.py         # Data extraction logic
│   ├── transform.py       # Data transformation layer
│   ├── load_curated.py    # Curated dataset preparation
│   └── load_to_s3.py      # Upload to Amazon S3
│
├── tests/
│   └── test_transform.py  # Unit tests for transformations
│
├── docker-compose.yaml    # Airflow local environment
├── requirements.txt       # Project dependencies
└── README.md
```

---

## ⚙️ Pipeline Design Principles

### 1️⃣ Modularization

Each ETL stage is isolated:

| Layer         | Responsibility                  |
| ------------- | ------------------------------- |
| Extract       | Data ingestion                  |
| Transform     | Cleaning & business rules       |
| Load          | Persist curated data            |
| Orchestration | Scheduling & dependency control |

This improves:

* Maintainability
* Testability
* Scalability

---

### 2️⃣ Incremental Processing

The pipeline processes only new data during each execution, simulating real production ingestion patterns and avoiding unnecessary reprocessing.

Benefits:

* Lower compute cost
* Faster execution
* Realistic data growth simulation

---

### 3️⃣ Idempotency

Pipeline executions are **safe to retry**.

If a task runs multiple times:

* No duplicated data
* No corrupted outputs
* Consistent results

This mirrors enterprise-grade pipeline reliability.

---

### 4️⃣ Logging & Observability

Structured logging enables:

* Execution tracking
* Debugging failures
* Monitoring processed records

Logs are integrated with Airflow task visibility.

---

### 5️⃣ Error Handling

The pipeline includes defensive mechanisms such as:

* Controlled exceptions
* Retry-safe operations
* Failure transparency through Airflow

---

## ⏰ Orchestration

The pipeline is orchestrated using **Apache Airflow** with:

* Daily schedule execution
* Task dependency management
* Automatic retries
* Execution history tracking

---

## ☁️ Data Loading (S3)

Curated datasets are stored in **Amazon S3**, simulating a Data Lake architecture.

Typical usage:

* Analytics consumption
* Downstream modeling
* BI integration

---

## 🧪 Testing

Unit tests validate transformation logic:

```
tests/test_transform.py
```

Ensures:

* Data consistency
* Transformation reliability
* Safer refactoring

---

## 🐳 Running Locally

### 1. Clone repository

```bash
git clone <repo-url>
cd airflow-project
```

### 2. Start Airflow environment

```bash
docker compose up -d
```

### 3. Access Airflow UI

```
http://localhost:8080
```

(Default credentials defined in docker configuration.)

---

## ▶️ Execute Pipeline

1. Enable DAG in Airflow UI
2. Trigger manually or wait for scheduled run
3. Monitor logs and task execution

---

## 📈 Engineering Concepts Demonstrated

* ETL orchestration
* Incremental ingestion
* Data Lake loading
* Idempotent pipelines
* Modular Python architecture
* Logging strategy
* Airflow scheduling
* Test-driven transformations

---

## 🎯 Project Purpose

This repository represents a **portfolio-ready Data Engineering project**, showcasing how modern pipelines are structured in production environments.

It focuses on **engineering reliability, maintainability, and scalability**, rather than only data manipulation.

---

## 📬 Future Improvements

* Data quality checks
* Metadata tracking
* Partitioned datasets
* CI/CD integration
* Infrastructure as Code (IaC)

---

## 👨‍💻 Author

**Vinicius Santos**
Data Engineering Enthusiast | Building production-oriented data pipelines
