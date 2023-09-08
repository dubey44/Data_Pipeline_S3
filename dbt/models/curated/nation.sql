{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['nation_key', 'nation_name', 'region_key']
    )
}}


WITH
    raw_nation AS (
        SELECT  
            nationkey AS nation_key,
            nation AS nation_name,
            regionkey AS region_key,
            MAX(etl_ts) as etl_ts
        FROM {{ source('source_raw', 'order_summary') }}
        GROUP BY nation_key, nation_name,region_key
    )

SELECT *
FROM raw_nation
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}

