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
  {{ dbt_utils.generate_surrogate_key(['campaign_id', 'name', 'event_time']) }} as unique_key,
  campaign_id,
  origin_campaign_id,
  is_recurring,
  name,
  timezone,
  version_name,
  language,
  program_id,
  program_version_id,
  suite_type,
  suite_event,
  campaign_type,
  defined_type,
  category_name,
  event_time,
  customer_id,
  partitiontime,
  loaded_at,
  subject
from {{ source('emarsys_brandalley_523470888', 'email_campaigns_v2_523470888') }}
where 1=1
{% if is_incremental() %}
  and date(partitiontime) >= current_date - 1
{% endif %}



