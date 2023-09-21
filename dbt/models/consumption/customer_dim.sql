{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['cust_id' ]
    )
}}

WITH customer AS (
    SELECT * FROM {{ ref('customer') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['cust_key', 'cust_name', 'cust_address','cust_phone']) }} as cust_id,
    cust_key,
    cust_name,
    cust_address,
    cust_phone,
    cust_mktsegment,
    etl_ts
FROM customer
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}