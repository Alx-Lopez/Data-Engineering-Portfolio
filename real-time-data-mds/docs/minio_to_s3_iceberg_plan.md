# MinIO to S3 + Iceberg Migration Plan

## Goals
- Keep existing Parquet landing layout while switching storage from MinIO to S3.
- Introduce Iceberg tables for Databricks/Spark ML workflows.
- Preserve Snowflake compatibility (optional) via external tables or direct COPY.

## Phase 0: Prep
- Choose AWS account/region and create S3 buckets for `bronze/`, `silver/`, `gold/`, `dlq/`, `quarantine/`.
- Set up Glue catalog (or Iceberg REST catalog) for Databricks.
- Decide warehouse locations (e.g., `s3://<bucket>/warehouse/`).

## Phase 1: Storage Cutover
- Update `.env.docker`/`.env.local` to point to S3 endpoints/credentials.
- If staying on MinIO temporarily, mirror data to S3 using `mc mirror` or `aws s3 sync`.
- Validate that new Parquet landings arrive under:
  - `s3://<bucket>/topic=stock-quotes/dt=YYYY-MM-DD/hr=HH/partition=<p>/...`

## Phase 2: Iceberg Bronze
- Create Iceberg Bronze table in Databricks using `databricks/iceberg_bronze.sql`.
- Configure table properties for schema evolution and file compaction.

## Phase 3: Silver/Gold
- Build Silver table with:
  - Deduplication by `event_id` + `source_system`.
  - Type casting and null handling.
- Build Gold/Feature tables for ML training/inference windows.

## Phase 4: Ops & Governance
- Schedule Iceberg compaction jobs (optimize/auto-compaction).
- Set data retention policies for `dlq/` and `quarantine/`.
- Add monitoring for schema drift and DLQ volume spikes.

## Validation
- Compare record counts between MinIO and S3 for a fixed time window.
- Run sampling checks on Bronze/Silver for schema and null rates.
