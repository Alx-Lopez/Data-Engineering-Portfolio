# Real-Time Stock Market Data Pipeline

## Overview
This project is an end-to-end data engineering solution designed to ingest, process, and visualize high-frequency stock market data in real-time. It demonstrates a **Lambda Architecture** approach, utilizing **Apache Kafka** for streaming ingestion and **Snowflake** for robust warehousing and analytics.

The goal of this project was to simulate a production-grade environment where data reliability, scalability, and latency are critical factors.

## Architecture
![Pipeline Architecture]([INSERT_LINK_TO_YOUR_DIAGRAM_IMAGE_HERE])

The pipeline consists of the following stages:
1.  **Ingestion:** Python scripts fetch real-time data from financial APIs.
2.  **Streaming:** **Apache Kafka** acts as the high-throughput message broker to buffer and decouple data producers from consumers.
3.  **Orchestration:** **Apache Airflow** (running in Docker) manages the workflow dependencies and scheduling.
4.  **Storage (Data Lake):** Raw JSON events are archived in an **MinIO S3** bucket for durability and replayability.
5.  **Warehousing:** Data is loaded into **Snowflake** (Raw Layer).
6.  **Transformation:** **dbt (data build tool)** performs modular transformations to move data from *Raw* to *Cleaned* to *Business Ready* (Medallion Architecture).
7.  **Visualization:** **Power BI** connects to the serving layer for near real-time dashboards.

## Tech Stack
* **Language:** Python 3.12+
* **Containerization:** Docker & Docker Compose
* **Orchestration:** Apache Airflow
* **Streaming:** Apache Kafka & Zookeeper
* **Data Lake:** AWS S3
* **Data Warehouse:** Snowflake
* **Transformation:** dbt Core
* **BI/Visualization:** Power BI

## Key Features
* **Containerized Environment:** The entire infrastructure (Airflow, Kafka, Zookeeper) is defined in `docker-compose.yml` for reproducible deployments.
* **Medallion Architecture:** Implements a structured data flow (Bronze/Silver/Gold) within Snowflake to ensure data quality and governance.
* **Fault Tolerance:** Kafka acts as a buffer to handle backpressure and prevent data loss during API spikes.
* **Infrastructure as Code:** All warehouse tables and pipes are managed via SQL scripts and dbt models.

## Setup & Installation

### Prerequisites
* Docker Desktop installed
* AWS Account (S3 access)
* Snowflake Account
* Python 3.9+

### Running the Pipeline
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/Alx-Lopez/stock-market-pipeline.git](https://github.com/Alx-Lopez/stock-market-pipeline.git)
    cd stock-market-pipeline
    ```

2.  **Configure Credentials:**
    Create a `.env` file containing your API keys and Snowflake credentials:
    ```env
    AWS_ACCESS_KEY=...
    SNOWFLAKE_USER=...
    API_KEY=...
    ```

3.  **Start Services:**
    Spin up the Docker containers for Airflow and Kafka:
    ```bash
    docker-compose up -d
    ```

4.  **Trigger the DAG:**
    Access the Airflow UI at `http://localhost:8080` and enable the `stock_data_ingestion` DAG.

## Future Improvements
* Implement **Spark Streaming** for complex windowed aggregations before loading to Snowflake.
* Add **Great Expectations** for more granular data quality testing within the Airflow DAG.
* Integrate CI/CD using GitHub Actions for automated dbt testing.
* Store Kafka events in S3 as partitioned Parquet/Avro with deterministic offset buckets, compression, and compaction to manage small files.
* Look to Deploy in Cloud Solution  

## Near-Real-Time Defaults
Set these in `ifra/.env` to target ~15s end-to-end latency:
```
S3_BASE_PREFIX=
PARQUET_COMPRESSION=snappy
BATCH_SIZE=500
MAX_BATCH_SECONDS=10
MAX_BATCH_BYTES=1048576
OFFSET_BUCKET_SIZE=500
MAX_BUCKET_SECONDS=15
```
Tradeoff: lower latency means smaller files. Plan to run periodic compaction to keep query performance healthy.
Dependencies: the compaction script and any downstream querying expect the partitioned layout `topic=<t>/dt=YYYY-MM-DD/hr=HH/partition=<p>/` and deterministic `start=<offset>_end=<offset>.parquet` filenames.

## Idempotent Load Design (Snowflake)
To make the pipeline idempotent end-to-end:
- Producer emits a deterministic `event_id` and `source_system` per event.
- Airflow loads files into a Snowflake stage, copies into a raw bronze table with `load_ts` and `file_name` lineage, then `MERGE`s into a canonical bronze table keyed by `event_id` + `source_system`.
- The merge deduplicates by keeping the latest `load_ts` per event key.

## Schema Validation and Data Quality
- Ingestion validates events against `ifra/consumer/schema/stock_quote.schema.json` (path is resolved relative to the repo); invalid events are sent to a DLQ prefix (default `dlq/`).
- Configure validation via `SCHEMA_VALIDATION_ENABLED`, `SCHEMA_FILE`, `DLQ_PREFIX`, and `QUARANTINE_PREFIX`.
- dbt enforces contracts/tests on the bronze staging model (`not_null`, `unique`, `accepted_values`) to catch schema drift.

## Bad Data Paths (DLQ + Quarantine + Replay)
- Invalid events are published to a Kafka dead-letter topic (`KAFKA_DLQ_TOPIC`) and written to object storage under `dlq/` and `quarantine/`.
- Replay quarantined records by running `python real-time-data-mds/ifra/scripts/replay_quarantine.py` with:
  - `REPLAY_MODE=kafka` (default) and `REPLAY_TOPIC`, or
  - `REPLAY_MODE=s3` and `REPLAY_S3_PREFIX` to re-land into storage.
- `REPLAY_HOURS` limits replay to the last N hours (set `0` to replay everything).
- Airflow automation: enable the `replay_quarantine` DAG to replay once per hour.

## Troubleshooting Notes
### Challenges Faced
- Kafka local connectivity required correct advertised listeners and using `localhost:29092` from host processes.
- Airflow webserver failures due to a stale PID file when Gunicorn did not shut down cleanly.
- Airflow tasks couldn't read local `.env` until it was mounted inside the containers.
- MinIO endpoint needed Docker service DNS (`http://minio:9000`) instead of `localhost` when running inside Airflow.
- Snowflake permissions blocked dbt models on schema `COMMON` until grants were applied for the role in use.
- dbt source/model parsing errors caused by YAML formatting and incorrect `source()` Jinja syntax.
- JSON field names in `bronze_stock_quotes_raw` did not match model expectations, producing NULLs.
- Kafka hot partition can be avoided by using round-robin partitioning (no key) or by increasing key cardinality.
- To reset a topic, delete and recreate it with the desired partition count using `kafka-topics`.
- Slow consumer throughput can be caused by small batch sizes, frequent flushes, single-partition topics, slow S3/MinIO writes, or low local resources (CPU/disk).
- If all messages land in partition 0, the topic likely has only 1 partition; recreate it with more partitions and keep a non-empty key (salted if needed).
- If the topic has multiple partitions but all records still land in partition 0, remove the producer key to use round-robin, or increase key cardinality.
- If round-robin still lands on one partition, the topic likely has only 1 partition or the producer wasn't restarted after config changes.
- Kafka will return “topic already exists” when recreating; use `--alter` to increase partitions, or delete the topic first (partition count cannot be decreased).
- Airflow tasks must use the Docker service DNS for MinIO (`http://minio:9000`), not `localhost`, to avoid connection refused errors.
- Use `ifra/.env.docker` for containers and `ifra/.env.local` for running producers/consumers on your host.
- Snowflake load can fail with `invalid identifier 'LOAD_TS'` if older tables were created without new columns; the DAG now adds missing columns automatically.
- To clear the Snowflake stage manually, run `python real-time-data-mds/ifra/scripts/clear_snowflake_stage.py`.
- To purge old JSON files in an internal stage: `ENV_FILE=real-time-data-mds/ifra/.env.local STAGE_PATTERN='.*\\.json' python real-time-data-mds/ifra/scripts/clear_snowflake_stage.py`.


### Important Commands
- Docker services:
  - `docker compose up -d zookeeper kafka`
  - `docker compose up -d --force-recreate airflow-webserver airflow-scheduler airflow-init`
  - `docker compose logs --tail 200 airflow-webserver`
- Airflow (stale PID cleanup):
  - `docker compose exec airflow-webserver rm -f /opt/airflow/airflow-webserver.pid`
  - `docker compose restart airflow-webserver`
- Airflow user creation:
  - `docker compose exec airflow-webserver airflow users create --username admin --firstname ### --lastname ### --role Admin --email a@example.com --password <password>`
- dbt (local profile):
  - `cd real-time-data-mds/dbt_stocks`
  - `set -a; source ../ifra/.env; set +a`
  - `dbt run`
  - `dbt run --debug`
- Snowflake grants for `COMMON` schema (run as admin role):
  - `GRANT USAGE ON DATABASE STOCKS_MDS TO ROLE PUBLIC;`
  - `GRANT USAGE ON SCHEMA STOCKS_MDS.COMMON TO ROLE PUBLIC;`
  - `GRANT CREATE TABLE ON SCHEMA STOCKS_MDS.COMMON TO ROLE PUBLIC;`
  - `GRANT CREATE VIEW ON SCHEMA STOCKS_MDS.COMMON TO ROLE PUBLIC;`
  - `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA STOCKS_MDS.COMMON TO ROLE PUBLIC;`
  - `GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA STOCKS_MDS.COMMON TO ROLE PUBLIC;`
  - `GRANT USAGE ON ALL STAGES IN SCHEMA STOCKS_MDS.COMMON TO ROLE PUBLIC;`
  - `GRANT USAGE ON FUTURE STAGES IN SCHEMA STOCKS_MDS.COMMON TO ROLE PUBLIC;`
