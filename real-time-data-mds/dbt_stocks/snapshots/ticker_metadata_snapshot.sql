{% snapshot ticker_metadata_snapshot %}

{{
  config(
    target_schema='COMMON',
    unique_key='ticker',
    strategy='timestamp',
    updated_at='updated_at'
  )
}}

SELECT
  ticker,
  company_name,
  sector,
  industry,
  exchange,
  updated_at
FROM {{ ref('ticker_metadata') }}

{% endsnapshot %}
