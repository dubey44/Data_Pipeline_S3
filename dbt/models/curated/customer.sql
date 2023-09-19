
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['cust_key','cust_name','cust_address', 'cust_phone', 'cust_mktsegment', 'nation_key']
    )
}}



WITH
    raw_customer AS (
        SELECT
            custkey AS cust_key,
            customer AS cust_name,
            cust_address AS cust_address,
            phone AS cust_phone,
            cust_acctbal AS cust_acctbal,
            cust_mktsegment AS cust_mktsegment,
            nationkey AS nation_key,
            MAX(etl_ts) as etl_ts
        FROM {{ source('source_raw', 'order_summary') }}
        GROUP BY cust_key,cust_name,cust_address,cust_phone,cust_acctbal,cust_mktsegment,nation_key
    )

SELECT *
FROM raw_customer
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}

