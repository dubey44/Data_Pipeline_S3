
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['supp_id' ]
    )
}}

WITH supplier AS (
    SELECT * FROM {{ ref('supplier') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['supp_key', 'supp_name', 'supp_phone','supp_address', 'supplier_comment']) }} as supp_id,
    supp_key,
    supp_name,
    supp_phone,
    supp_address,
    supplier_comment as supp_comment,
    etl_ts
FROM supplier
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}





