{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['line_item_id' ]
    )
}}

WITH line_item AS (
    SELECT * FROM {{ ref('line_item') }}
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
    {{ dbt_utils.generate_surrogate_key(['order_id', 'part_id', 'supp_id','line_number', 'line_item_comment']) }} as line_item_id,
    order_id,
    part_id,
    supp_id,
    line_number,
    return_flag,
    line_status,
    ship_date,
    commit_date,
    receipt_date,
    ship_instruct,
    ship_mode,
    line_item_comment,
    line_item.etl_ts as etl_ts
FROM line_item, part_dim, supplier_dim, orders_dim
WHERE line_item.order_key = orders_dim.order_key AND    
      line_item.part_key = part_dim.part_key AND
      line_item.supply_key = supplier_dim.supp_key
{% if is_incremental() %}
    AND line_item.etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}

