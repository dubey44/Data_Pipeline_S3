{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['order_id', 'region_id', 'nation_id', 'cust_id' ]
    )
}}

WITH customer_dim AS (
    SELECT * FROM {{ ref('customer_dim') }}
),
orders_dim AS (
    SELECT * FROM {{ ref('orders_dim') }}
),
region_dim AS (
    SELECT * FROM {{ ref('region_dim') }}
),
nation_dim AS (
    SELECT * FROM {{ ref('nation_dim') }}
),
customer AS (
    SELECT * FROM {{ ref('customer') }}
),
orders AS (
    SELECT * FROM {{ ref('orders') }}
),
nation AS (
    SELECT * FROM {{ ref('nation') }}
)

SELECT
    cust_id,
    order_id,
    region_id,
    nation_id,
    total_price,
    orders.etl_ts AS etl_ts
FROM customer_dim, orders_dim, region_dim, nation_dim, customer, orders, nation
WHERE orders_dim.order_key = orders.order_key AND
      customer_dim.cust_key = orders.cust_key AND
      customer_dim.cust_key = customer.cust_key AND
      customer.nation_key = nation.nation_key AND
      nation.nation_key = nation_dim.nation_key AND
      nation.region_key = region_dim.region_key
{% if is_incremental() %}
    AND orders.etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}



