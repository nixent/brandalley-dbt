
{{ config(
    full_refresh=false,
    on_schema_change='sync_all_columns',
    materialized='incremental',
    tags=["job_daily"]
) }}

select 
    current_timestamp as processed_at,
    * 
from {{ ref('stock_age') }}
{% if is_incremental() %}
where current_date > (select max(logged_date) from {{this}})
{% endif %}