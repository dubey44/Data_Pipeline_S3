
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['part_id' ]
    )
}}


WITH part AS (
    SELECT * FROM {{ ref('part') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['part_key', 'part_name', 'part_manufacturer','part_brand', 'part_size']) }} as part_id,
    part_key,
    part_name,
    part_manufacturer,
    part_brand,
    part_type,
    part_size,
    part_container,
    part_retail_price,
    part_comment,
    etl_ts
FROM part
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}