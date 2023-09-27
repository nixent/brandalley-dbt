{{config(
    materialized='incremental',
    unique_key='ba_site_entity_id',
	cluster_by='ba_site_entity_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    entity_id,
    entity_type_id,
    attribute_set_id,
    type_id,
    sku,
    has_options,
    required_options,
    timestamp(created_at) as created_at,
    timestamp(updated_at) as updated_at,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__catalog_product_entity') }}
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
    type_id,
    sku,
    has_options,
    required_options,
    created_at,
    updated_at,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__catalog_product_entity') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}
qualify row_number() over (partition by sku, ba_site order by updated_at desc) = 1

