{{config(
    materialized='incremental',
    unique_key='ba_site_entity_id_attribute_id',
	cluster_by='entity_id',
    partition_by = {
      "field": "bq_last_processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

select
    'UK-' || entity_id || '-' || attribute_id                       as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    value_id,
    entity_type_id,
    attribute_id,
    entity_id,
    value,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__customer_address_entity_varchar') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || entity_id || '-' || attribute_id                       as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    value_id,
    entity_type_id,
    attribute_id,
    entity_id,
    value,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__customer_address_entity_varchar') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}