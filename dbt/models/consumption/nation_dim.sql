{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'merge',
        unique_key= ['nation_id' ]
    )
}}

WITH nation AS (
    SELECT * FROM {{ ref('nation') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['nation_key', 'nation_name']) }} as nation_id,
    nation_key,
    nation_name,
    etl_ts
FROM nation
{% if is_incremental() %}
    WHERE etl_ts > (SELECT MAX(etl_ts) from {{this}})
{% endif %}
