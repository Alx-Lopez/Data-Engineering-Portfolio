# Governance & Security (Quick Wins)

## Data lineage and access patterns
- **Producers**: Finnhub API → Kafka topic
- **Storage**: Parquet in S3/MinIO (`topic=.../dt=.../hr=.../partition=...`)
- **Snowflake**: External stage → Bronze Raw → Bronze Canonical → dbt Silver/Gold
- **Databricks**: Bronze/Silver/Gold in Iceberg for ML use cases

Access patterns:
- BI dashboards query Snowflake Gold models.
- ML training reads Databricks Gold/feature tables.

## Least-privilege access
- **Airflow role**: only USAGE on warehouse/db/schema, and CRUD on Bronze tables + stage.
- **Databricks**: Storage Credential + External Location with READ FILES only.

## Auditing quick wins
- Snowflake query history for load times and user activity.
- S3 access logs (optional) for object-level access.
- Airflow task logs for load failures.

## Data contracts
- JSON schema validation at ingestion.
- dbt tests for not_null/unique/accepted_values.

## Security checklist
- [ ] Use storage integrations (no AWS keys in Airflow)
- [ ] Avoid embedding secrets in `.env` files
- [ ] Restrict access by role (Snowflake) and external location (Databricks)
