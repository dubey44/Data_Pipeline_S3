{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['part_id', 'region_id', 'nation_id', 'supp_id' ]
    )
}}

WITH part_supplier AS (
    SELECT * FROM {{ ref('part_supplier') }}
),
part_dim AS (
    SELECT * FROM {{ ref('part_dim') }}
),
supplier_dim AS (
    SELECT * FROM {{ ref('supplier_dim') }}
),
region_dim AS (
    SELECT * FROM {{ ref('region_dim') }}
),
nation_dim AS (
    SELECT * FROM {{ ref('nation_dim') }}
),
supplier AS (
    SELECT * FROM {{ ref('supplier') }}
),
nation AS (
    SELECT * FROM {{ ref('nation') }}
)

SELECT 
    part_id, 
    region_id,
    nation_id,
    supp_id,
    part_supp_cost,
    part_availability,
    part_supplier.etl_ts AS etl_ts
FROM part_supplier, part_dim, supplier_dim, region_dim, nation_dim, supplier, nation
WHERE part_supplier.part_key = part_dim.part_key AND
      part_supplier.supp_key = supplier_dim.supp_key AND
      supplier_dim.supp_key = supplier.supp_key AND
      supplier.supp_nation = nation_dim.nation_key AND
      region_dim.region_key = nation.region_key AND
      nation_dim.nation_key = nation.nation_key
{% if is_incremental() %}
    AND part_supplier.etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}

