{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["region_key", "region_name"],
    )
}}

with
    raw_region as (
        select regionkey as region_key, region as region_name, max(etl_ts) as etl_ts
        from {{ source('source_raw', 'order_summary') }}
        group by region_key , region_name
    )

SELECT *
from raw_region
WHERE region_key IS NOT NULL AND
      region_name IS NOT NULL
{% if is_incremental() %}
    AND etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
