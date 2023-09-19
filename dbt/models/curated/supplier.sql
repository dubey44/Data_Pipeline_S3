
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['supp_key', 'supp_name', 'supp_nation', 'supp_phone', 'supp_acc_bal', 'supp_address', 'supplier_comment' ]
    )
}}

WITH raw_supplier AS (
    SELECT 
        suppkey AS supp_key, 
        supplier_name AS supp_name,
        supplier_nation AS supp_nation,
        phone AS supp_phone,
        acctbal AS supp_acc_bal,
        address AS supp_address,
        supplier_comment AS supplier_comment,
        MAX(etl_ts) AS etl_ts
    FROM {{ source('source_raw', 'part_supply_supplier') }}
    GROUP BY supp_key,supp_name,supp_nation,supp_phone,supp_acc_bal,supp_address,supplier_comment
)
SELECT * FROM raw_supplier
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
