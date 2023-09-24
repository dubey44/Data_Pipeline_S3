{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['region_id' ]
    )
}}

WITH region AS (
    SELECT * FROM {{ ref('region') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['region_key', 'region_name']) }} as region_id,
    region_key,
    region_name,
    etl_ts
FROM region
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
