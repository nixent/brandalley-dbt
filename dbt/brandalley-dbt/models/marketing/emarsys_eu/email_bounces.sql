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
  bounce_type,
  campaign_id,
  campaign_type,
  contact_id,
  customer_id,
  domain,
  email_sent_at,
  event_time,
  launch_id,
  loaded_at,
  message_id,
  partitiontime
from {{ source('emarsys_brandalley_523470888', 'email_bounces_523470888') }}
where 1=1
{% if is_incremental() %}
  and date(partitiontime) >= current_date - 1
{% endif %}



