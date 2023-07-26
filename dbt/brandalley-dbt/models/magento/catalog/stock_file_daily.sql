
{{ config(
    full_refresh=false,
    on_schema_change='sync_all_columns',
    materialized='incremental'
) }}

select 
    current_date      as stock_file_date,
    current_timestamp as processed_at,
    * 
from {{ ref('stock_file') }}
{% if is_incremental() %}
where current_date > (select max(stock_file_date) from {{this}})
{% endif %}