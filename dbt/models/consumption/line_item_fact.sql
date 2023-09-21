{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['line_item_id', 'quantity', 'extended_price', 'discount', 'tax' ]
    )
}}

WITH line_item AS (
    SELECT * FROM {{ ref('line_item') }}
),
line_item_dim AS (
    select * FROM {{ ref('line_item_dim') }}
),
part_dim AS (
    SELECT * FROM {{ ref('part_dim') }}
),
orders_dim AS (
    SELECT * FROM {{ ref('orders_dim') }}
),
supplier_dim AS (
    SELECT * FROM {{ ref('supplier_dim') }}
)


SELECT 
    line_item_id,
    quantity,
    extended_price,
    discount,
    tax,
    line_item.etl_ts AS etl_ts
FROM line_item, line_item_dim, part_dim, orders_dim, supplier_dim
WHERE line_item.order_key = orders_dim.order_key AND
      line_item.supply_key = supplier_dim.supp_key AND
      line_item.part_key = part_dim.part_key AND
      line_item_dim.order_id = orders_dim.order_id AND
      line_item_dim.part_id = part_dim.part_id AND
      line_item_dim.supp_id = supplier_dim.supp_id
{% if is_incremental() %}
    AND line_item.etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
