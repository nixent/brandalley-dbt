{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'] %}

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by = {
      "field": "partitiontime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    partitions=partitions_to_replace
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
where 1=1
  -- for dev
  and date(partitiontime) >= current_date - 2 
{% if is_incremental() %}
  and date(partitiontime) >= current_date - 1
{% endif %}