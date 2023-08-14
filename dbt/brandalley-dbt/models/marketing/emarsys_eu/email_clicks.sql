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
    {# {{dbt_utils.generate_surrogate_key(['customer_id', 'message_id', 'launch_id'])}} as unique_key, #}
    contact_id,
    launch_id,
    domain,
    email_sent_at,
    campaign_type,
    geo,
    platform,
    md5,
    is_mobile,
    is_anonymized,
    uid,
    ip,
    user_agent,
    section_id,
    link_id,
    category_id,
    is_img,
    campaign_id,
    message_id,
    event_time,
    customer_id,
    partitiontime,
    loaded_at,
    category_name,
    link_name,
    link_analysis_name,
    relative_link_id
from {{ source('emarsys_brandalley_523470888', 'email_clicks_523470888') }}
where 1=1
{% if is_incremental() %}
  and date(partitiontime) >= current_date - 1
{% endif %}