
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key = ['order_key', 'part_key', 'supply_key','line_number','quantity', 'extended_price', 'discount', 'tax','return_flag', 'line_status', 'ship_date',
            'commit_date', 'receipt_date', 'ship_instruct', 'ship_mode', 'line_item_comment' ]
    )
}}


WITH raw_line_item AS (
    SELECT * FROM {{ source('source_raw', 'line_item_supply') }}
),
part_supplier AS (
    SELECT * FROM {{ ref('part_supplier') }}
),
orders AS (
    SELECT * FROM {{ ref('orders') }}
),
final AS(
SELECT 
    orderkey AS order_key_1,
    partkey AS part_key_1,
    suppkey AS supply_key,
    linenumber AS line_number,
    quantity AS quantity,
    extendedprice AS extended_price,
    discount AS discount,
    tax AS tax,
    returnflag AS return_flag,
    linestatus AS line_status,
    shipdate::DATE AS ship_date,
    commitdate::DATE AS commit_date,
    receiptdate::DATE AS receipt_date,
    shipinstruct AS ship_instruct,
    shipmode AS ship_mode,
    line_item_comment AS line_item_comment,
    MAX(raw_line_item.etl_ts) AS etl_ts
FROM raw_line_item, part_supplier, orders
WHERE raw_line_item.partkey = part_supplier.part_key AND 
      raw_line_item.suppkey = part_supplier.supp_key AND 
      raw_line_item.orderkey = orders.order_key
GROUP BY order_key_1,part_key_1,supply_key,line_number,quantity,extended_price,discount,tax,return_flag,line_status,ship_date,commit_date,receipt_date,ship_instruct,ship_mode,line_item_comment
)

SELECT 
    order_key_1 AS order_key,
    part_key_1 AS part_key,
    *
    FROM final
{% if is_incremental() %}
    where final.etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
