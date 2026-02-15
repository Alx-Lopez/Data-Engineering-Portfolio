# Observability Plan (Freshness, Anomalies, Lineage)

## Freshness SLAs
- Bronze: < 10 minutes
- Gold: < 60 minutes

dbt source freshness is configured in:
- `dbt_stocks/models/bronze/source.yml`

Run:
```bash
cd real-time-data-mds/dbt_stocks
dbt source freshness
```

## Row Count & Volume Anomalies
Add scheduled checks for:
- sudden drop in rows per hour
- spikes beyond expected volume
- missing tickers

Example ad‑hoc checks:
```sql
SELECT
  date_trunc('hour', load_ts) AS hour,
  COUNT(*) AS rows
FROM STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW
GROUP BY 1
ORDER BY 1 DESC;
```

## Schema Drift Alerts
- Use dbt tests (not_null/unique/accepted_values) to detect drift.
- For breaking changes, fail the pipeline and route invalid rows to DLQ/quarantine.

## Data Lineage Columns
Stored in Bronze raw and canonical tables:
- `file_name`
- `ingest_ts`
- `load_batch_id`
- `_kafka_partition`, `_kafka_offset` (from event payload)

## Centralized Metrics (Lightweight)
Track:
- failures by task (Airflow UI)
- Kafka consumer lag (Kafdrop)
- Snowflake load time (query history)
- storage cost (S3 bucket metrics)

## Snowflake Table Optimization (Clustering + Batching)
### Clustering strategy
For large fact tables, cluster on time and key dimensions to improve pruning.

Example:
```sql
ALTER TABLE STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES
CLUSTER BY (TO_DATE(INGEST_TS), TICKER);
```

### Micro-partition clustering
If queries filter heavily on event time, use `INGEST_TS` or `EVENT_TIME` as the leading cluster key.

### Small-file handling
Snowflake reads from external stages; it performs best with fewer, larger files.
Options:
- **Pre‑load compaction**: merge Parquet files in S3 before Snowflake `COPY` (recommended).
- **Batching**: write Parquet in larger batches at the producer/consumer side.

Recommended target sizes:
- 128MB–512MB per file for S3

### COPY best practices
Limit the scope of each load to a path prefix (hourly) to avoid reprocessing:
```sql
COPY INTO STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW (data, load_ts, file_name, ingest_ts, load_batch_id)
FROM (
  SELECT $1, CURRENT_TIMESTAMP(), METADATA$FILENAME, CURRENT_TIMESTAMP(), '<batch-id>'
  FROM @bronze_stock_quotes_stage
)
FILE_FORMAT = (TYPE=PARQUET)
PATTERN = '.*dt=2026-01-07/hr=18/.*\\.parquet';
```

### Optional: Snowflake auto‑clustering
If table size grows, enable auto clustering:
```sql
ALTER TABLE STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES
SET AUTO_CLUSTERING = TRUE;
```
