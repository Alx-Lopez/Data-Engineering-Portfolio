{{ config(materialized='table') }}

select
  ticker,
  company_name,
  sector,
  industry,
  exchange,
  dbt_valid_from as valid_from,
  dbt_valid_to as valid_to,
  case when dbt_valid_to is null then true else false end as is_current
from {{ ref('ticker_metadata_snapshot') }}
