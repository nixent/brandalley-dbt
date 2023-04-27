{{config(
    materialized='incremental',
    unique_key='ba_site_subscriber_id',
	  cluster_by=['customer_id', 'subscriber_id'],
    partition_by = {
      "field": "bq_last_processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    subscriber_id,
    store_id,
    timestamp(change_status_at) as change_status_at,
    customer_id,
    subscriber_email,
    subscriber_status,
    subscriber_confirm_code,
    timestamp(subscription_date) as subscription_date,
    pure360_sync_status,
    emarsys_sync_status,
    subscriber_source,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__newsletter_subscriber') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    subscriber_id,
    store_id,
    change_status_at,
    customer_id,
    subscriber_email,
    subscriber_status,
    subscriber_confirm_code,
    timestamp(null),
    pure360_sync_status,
    emarsys_sync_status,
    subscriber_source,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__newsletter_subscriber') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}