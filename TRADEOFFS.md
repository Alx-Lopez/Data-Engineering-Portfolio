# Tradeoffs and Architectural Decisions

## Scope and Goals
- Prioritize a production-like flow (streaming → lake → warehouse → BI/ML).
- Balance correctness, latency, and operational simplicity.

## Key Decisions and Tradeoffs

### Kafka as the Buffer
**Decision:** Use Kafka between producer and consumer.  
**Tradeoff:** Adds infra complexity, but provides backpressure handling and decouples ingestion from storage.

### Parquet Landing in Object Storage
**Decision:** Write Parquet files to S3/MinIO.  
**Tradeoff:** Efficient for analytics, but requires careful file sizing and compaction to avoid small-file overhead.

### Snowflake as the Primary BI Engine
**Decision:** Keep Silver/Gold in Snowflake for dashboards.  
**Tradeoff:** Fast BI performance and governance, but duplicates some transformations when Databricks is used for ML.

### Databricks for ML (Optional)
**Decision:** Use Databricks/Iceberg for ML workflows.  
**Tradeoff:** Enables Spark-based feature engineering, but adds a second compute plane to maintain.

### Idempotent Load Pattern
**Decision:** Use `event_id` + `source_system` with MERGE into canonical Bronze.  
**Tradeoff:** Extra merge cost, but ensures reprocessing is safe and deterministic.

### External Stage vs Download
**Decision:** Snowflake loads directly from S3 external stage.  
**Tradeoff:** Simpler and faster than download/PUT, but requires storage integration setup.

### Validation at Ingestion (JSON Schema)
**Decision:** Validate events before storage; route invalid to DLQ/quarantine.  
**Tradeoff:** Catches bad data early, but adds runtime overhead and requires schema maintenance.

### Orchestration Boundaries
**Decision:** Airflow orchestrates; Snowflake does set-based transforms; dbt does modeling/tests.  
**Tradeoff:** Clear ownership and scalability, but requires learning multiple tools.

### Incremental Processing (Tasks/Streams)
**Decision:** Use Snowflake Tasks/Streams for incremental merge.  
**Tradeoff:** Reduces Airflow micro-steps, but introduces Snowflake-native scheduling to manage.

## Operational Choices
- **Latency vs file size:** Smaller batches reduce latency but create more files.
- **Governance:** External stages + role-based grants reduce credential sprawl but require up-front setup.

## Next Improvements
- Automated compaction schedule for Parquet files.
- More robust data contracts and schema versioning.
- Cost monitoring and query optimization on Snowflake.
