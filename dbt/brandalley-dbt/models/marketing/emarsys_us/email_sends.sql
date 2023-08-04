{{ config(
    materialized='incremental',
    schema='emarsys_opendata_eu',
    partition_by = {
      "field": "loaded_at",
      "data_type": "timestamp",
      "granularity": "day"
    }    
)}}

select 
    {{dbt_utils.generate_surrogate_key(['customer_id', 'message_id', 'launch_id'])}} as unique_key,
    contact_id, 
    launch_id, 
    campaign_type, 
    domain, 
    campaign_id, 
    message_id, 
    event_time, 
    customer_id, 
    partitiontime, 
    loaded_at 
from {{ source('emarsys_brandalley_523470888', 'email_sends_523470888') }}
{% if is_incremental() %}
where date(partitiontime) >= current_date - 1 and loaded_at > (select max(loaded_at) from {{this}})
{% endif %}