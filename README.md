# News Popularity Data Engineering Pipeline

An end-to-end batch data engineering project that processes news popularity data using **AWS S3, Python, PySpark, Apache Airflow, Docker, PostgreSQL, and SQL**.

The project demonstrates data ingestion, data quality validation, batch transformation, data warehousing, and workflow orchestration.

## 1. Project Overview

This project uses the **Online News Popularity dataset (UCI Machine Learning Repository)** to simulate a batch-oriented data engineering workflow.

The pipeline follows a **Bronze–Silver–Gold architecture**:

- **Bronze:** Store raw CSV batches in AWS S3.
- **Ingestion:** Download selected batches from S3 using Python and boto3.
- **Silver:** Validate and transform data using PySpark.
- **Quarantine:** Isolate records that fail data quality checks.
- **Gold:** Load clean records into a PostgreSQL dimensional data warehouse.
- **Orchestration:** Use Apache Airflow and Docker to execute local batch transformations and Gold loading.

## 2. Architecture

```text
UCI Online News Popularity Dataset
                 |
                 v
      Synthetic Batch Generator
                 |
                 v
         AWS S3 (Bronze)
                 |
                 | Python + boto3
                 v
         Local Bronze CSV
                 |
                 | Apache Airflow + Docker
                 v
         PySpark Validation
            /          \
           v            v
    Silver Parquet   Quarantine Parquet
           |
           v
    PostgreSQL Gold Warehouse
           |
           v
       SQL Analytics
```

**Orchestration scope:** The Airflow DAG orchestrates local Bronze-to-Silver transformations and Silver-to-Gold loading. S3 ingestion is currently executed separately through a Python CLI.

## 3. Technology Stack

| Component | Technology |
|---|---|
| Programming | Python |
| Cloud storage | AWS S3 |
| Cloud ingestion | boto3 |
| Data processing | PySpark |
| File format | CSV, Parquet |
| Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |
| Data warehouse | PostgreSQL |
| Analytics | SQL |
| Version control | Git, GitHub |

## 4. Data Quality and Processing Results

Three simulated daily batches were processed, each containing 1,000 records.

| Metric | Result |
|---|---:|
| Total input records | 3,000 |
| Valid Silver records | 2,940 |
| Quarantined records | 60 |
| Gold warehouse records | 2,940 |
| Duplicate URLs in Gold | 0 |

Implemented validation rules include:

- Positive article share counts.
- Sentiment values within the expected range.
- Valid weekday one-hot encoding.
- Duplicate URL detection.
- Record-count reconciliation between Bronze, Silver, and Quarantine.

Invalid records are separated into Quarantine Parquet datasets rather than silently discarded.

## 5. Airflow Orchestration

The primary DAG is `news_silver_to_gold`.

```text
transform_20260101 ─┐
transform_20260102 ─┼──> load_gold
transform_20260103 ─┘
```

The three batch transformation tasks can execute in parallel. The Gold loading task starts after all upstream transformations succeed.

**Verified execution:** All four tasks completed successfully.

## 6. Gold Data Warehouse

The Gold layer uses a dimensional model with the following tables:

- `gold.fact_news_popularity`
- `gold.dim_channel`
- `gold.dim_sentiment`
- `gold.dim_weekday`

The fact table supports downstream SQL analysis of news popularity across channels, sentiment categories, and weekdays.

The final validation confirmed **2,940 records, 2,940 distinct URLs, and zero duplicate URLs**.

## 7. Repository Structure

```text
dags/                    Airflow DAG definitions
docker/                  Docker image configuration
docs/                    Data quality documentation
sql/                     Gold schema and analytics SQL
src/
  generator/              Batch data generation
  ingestion/              AWS S3 download
  profiling/              Source data profiling
  transformation/         PySpark Silver and Quarantine
  loading/                PostgreSQL Gold loading
tests/                   Validation and integration tests
docker-compose.yaml      Local Airflow and ETL services
requirements.txt         Python dependencies
```

## 8. Running the Project

### Prerequisites

- Python and the dependencies in `requirements.txt`
- Docker Desktop and Docker Compose
- AWS credentials with access to the configured S3 bucket
- PostgreSQL with the Gold database and schema initialized
- An Apache Airflow connection for the Gold database

### Workflow

1. Generate CSV batches using the Python batch generator.
2. Upload the batches to the configured AWS S3 Bronze bucket.
3. Download the required batches locally using `src/ingestion/download_s3.py`.
4. Start the local services with Docker Compose.
5. Configure the PostgreSQL Gold connection in Airflow.
6. Trigger the `news_silver_to_gold` DAG.
7. Run the SQL analytics queries against the Gold warehouse.

**Local development note:** The current setup includes machine-specific Docker bind-mount paths and local PostgreSQL configuration. These must be adjusted for another environment. Docker socket access and development-only Airflow credentials are not intended for production deployment.

## 9. Key Learnings

This project provided hands-on experience with:

- Designing a layered batch data pipeline.
- Working with AWS S3 and Python-based ingestion.
- Implementing data quality rules and quarantine handling in PySpark.
- Writing partitioned Parquet datasets.
- Building a dimensional PostgreSQL data warehouse.
- Orchestrating dependent ETL tasks with Apache Airflow.
- Running and troubleshooting ETL workloads with Docker.
- Validating pipeline output using SQL and record reconciliation.

## 10. Future Improvements

- Integrate S3 ingestion directly into the Airflow DAG.
- Replace machine-specific paths with portable configuration.
- Add automated pipeline tests and CI/CD.
- Improve monitoring, alerting, and operational logging.
