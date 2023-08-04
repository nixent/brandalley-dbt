{{ config(
    materialized='incremental',
    partition_by = {
      "field": "loaded_at",
      "data_type": "timestamp",
      "granularity": "day"
    }    
)}}

select 
    {# uid as unique_key, #}
    contact_id, 
    launch_id,
    domain,
    email_sent_at,
    campaign_type,
    {# geo, #}
    platform,
    {# md5, #}
    is_mobile,
    is_anonymized,
    {# ip, #}
    user_agent,
    {# generated_from,  #}
    campaign_id, 
    message_id, 
    event_time, 
    customer_id, 
    loaded_at 
from {{ source('emarsys_brandalley_523470888', 'email_opens_523470888') }}
where 1=1
  -- for dev
  and date(partitiontime) >= current_date - 2 
{% if is_incremental() %}
  and date(partitiontime) >= current_date - 1 and loaded_at > (select max(loaded_at) from {{this}})
{% endif %}
