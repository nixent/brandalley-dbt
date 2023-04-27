{{config(
    materialized='incremental',
    unique_key='ba_site_entity_id',
	cluster_by='entity_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    entity_id,
    parent_id,
    row_total,
    price,
    weight,
    qty,
    product_id,
    order_item_id,
    additional_data,
    description,
    name,
    sku,
    sap_id,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__sales_flat_shipment_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    entity_id,
    parent_id,
    row_total,
    price,
    weight,
    qty,
    product_id,
    order_item_id,
    additional_data,
    description,
    name,
    sku,
    sap_id,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__sales_flat_shipment_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}
