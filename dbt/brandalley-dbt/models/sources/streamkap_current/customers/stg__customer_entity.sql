{{config(
    materialized='incremental',
    unique_key='ba_site_entity_id',
	  cluster_by='entity_id',
    partition_by = {
      "field": "bq_last_processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    entity_id,
    entity_type_id,
    attribute_set_id,
    website_id,
    email,
    group_id,
    increment_id,
    store_id,
    timestamp(created_at) as created_at,
    timestamp(updated_at) as updated_at,
    is_active,
    disable_auto_group_change,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__customer_entity') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    entity_id,
    entity_type_id,
    attribute_set_id,
    website_id,
    email,
    group_id,
    increment_id,
    store_id,
    created_at,
    updated_at,
    is_active,
    disable_auto_group_change,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__customer_entity') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}