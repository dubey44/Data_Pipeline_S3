
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['part_key', 'supp_key', 'part_supp_cost', 'part_availability', 'part_supp_comment' ]
    )
}}

WITH raw_part_supplier AS (
    SELECT 
        partkey AS part_key,
        suppkey AS supp_key,
        supplycost AS part_supp_cost,
        availqty AS part_availability,
        part_supplier_comment AS part_supp_comment,
        MAX(etl_ts) AS etl_ts
    FROM {{ source('source_raw', 'part_supply_supplier') }}
    GROUP BY part_key,supp_key,part_supp_cost, part_availability, part_supp_comment
)

SELECT * FROM raw_part_supplier
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
