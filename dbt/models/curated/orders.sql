{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['order_key', 'order_status', 'total_price', 'order_date', 'order_priority', 'clerk', 'order_comment', 'cust_key' ]
    )
}}


WITH raw_order AS (
    SELECT 
        orderkey AS order_key,
        orderstatus AS order_status,
        totalprice AS total_price,
        orderdate::DATE AS order_date,
        orderpriority AS order_priority,
        clerk AS clerk,
        shippriority as ship_priority,
        order_comment AS order_comment,
        custkey AS cust_key,
        MAX(etl_ts) AS etl_ts
    FROM {{ source('source_raw', 'order_summary') }}
    GROUP BY order_key, order_status, total_price, order_date,order_priority,clerk,ship_priority,order_comment,cust_key
)
SELECT * FROM raw_order
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
