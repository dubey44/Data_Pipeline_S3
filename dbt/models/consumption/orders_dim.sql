
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['order_id' ]
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('orders') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['order_key', 'order_status', 'order_date','order_priority', 'order_comment']) }} as order_id,
    order_key,
    order_status,
    order_date,
    order_priority,
    clerk,
    order_comment,
    ship_priority,
    etl_ts
FROM orders
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}

