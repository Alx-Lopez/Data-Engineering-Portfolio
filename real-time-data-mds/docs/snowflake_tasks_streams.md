# Snowflake Tasks + Streams (Incremental Processing)

Use Snowflake-native Tasks/Streams after landing raw data, while Airflow orchestrates high-level workflows.

## Boundaries
- Airflow: scheduling, dependency mgmt, retries, backfills, alerting
- Snowflake: bulk load + set-based operations (COPY/MERGE)
- dbt: transformations, tests, docs

## Example: Stream on Bronze Raw
```sql
ALTER TABLE STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW SET CHANGE_TRACKING = TRUE;
CREATE OR REPLACE STREAM bronze_stock_quotes_raw_stream
ON TABLE STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW
APPEND_ONLY = TRUE;
```

## Example: Task to Merge Incrementally
```sql
CREATE OR REPLACE TASK merge_bronze_stock_quotes
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
MERGE INTO STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES AS tgt
USING (
  SELECT
    data:event_id::STRING AS event_id,
    COALESCE(data:source_system::STRING, 'finnhub') AS source_system,
    data,
    CURRENT_TIMESTAMP() AS load_ts,
    METADATA$FILENAME AS file_name
  FROM STOCKS_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW_STREAM
) AS src
ON tgt.event_id = src.event_id AND tgt.source_system = src.source_system
WHEN MATCHED THEN UPDATE SET
  data = src.data,
  load_ts = src.load_ts,
  file_name = src.file_name
WHEN NOT MATCHED THEN INSERT (
  event_id, source_system, data, load_ts, file_name
) VALUES (
  src.event_id, src.source_system, src.data, src.load_ts, src.file_name
);
```

## Start the task
```sql
ALTER TASK merge_bronze_stock_quotes RESUME;
```

## Notes
- Use Tasks for incremental processing when data lands; let Airflow orchestrate the workflow, not each micro-step.
