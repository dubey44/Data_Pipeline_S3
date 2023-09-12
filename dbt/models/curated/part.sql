
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['part_key', 'part_name', 'part_manufacturer', 'part_brand', 'part_type', 'part_size', 'part_container', 
        'part_retail_price', 'part_comment' ]
    )
}}

WITH raw_part AS (
    SELECT  
        partkey AS part_key,
        part_name AS part_name,
        MFGR AS part_manufacturer,
        brand AS part_brand,
        type AS part_type,
        size AS part_size,
        container AS  part_container,
        retailprice AS part_retail_price,
        part_comment AS part_comment,
        MAX(etl_ts) as etl_ts
    FROM {{ source('source_raw', 'part_supply_supplier') }}
    GROUP BY part_key,part_name,part_manufacturer,part_brand,part_type,part_size,part_container,part_retail_price,part_comment
)

SELECT * FROM raw_part
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
