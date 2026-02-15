# Getting Started (Quick Start)

This repo contains a real‑time stock data pipeline with Kafka → Parquet → Snowflake and optional Databricks/Iceberg.

## Prerequisites
- Docker Desktop
- Python 3.12+
- AWS CLI (if using AWS S3)
- Snowflake account

## Quick Start (Local)
1) Fill in envs:
   - `real-time-data-mds/ifra/.env.local` (local runs)
   - `real-time-data-mds/ifra/.env.docker` (Airflow containers)
2) Start infra:
   - `cd real-time-data-mds/ifra`
   - `docker compose up -d`
3) Run producer (Kafka):
   - `cd real-time-data-mds/ifra/producer`
   - `uv run python producer.py`
4) Run consumer (S3/MinIO):
   - `cd real-time-data-mds/ifra/consumer`
   - `ENV_FILE=../.env.local uv run python consumer.py`
5) Trigger Snowflake load:
   - Airflow UI → enable `minio_to_snowflake`

## AWS S3 (Local Access)
- Setup guide: `real-time-data-mds/docs/local_s3_from_local.md`
- Snowflake external stage: `real-time-data-mds/docs/snowflake_storage_integration.md`

## Databricks (Optional)
- Notebook stub: `real-time-data-mds/databricks/iceberg_bronze.sql`

## Checklist
- [ ] `.env.local` filled in with real credentials
- [ ] `.env.docker` filled in with Snowflake + stage settings
- [ ] Kafka + Airflow containers running
- [ ] Producer running and writing Kafka events
- [ ] Consumer writing Parquet to S3/MinIO
- [ ] Snowflake external stage can read S3 path
- [ ] `minio_to_snowflake` DAG succeeds
- [ ] Databricks reads S3 path (optional)
