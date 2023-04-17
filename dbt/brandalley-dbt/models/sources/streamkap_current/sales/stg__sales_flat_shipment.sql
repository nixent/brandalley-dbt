{{config(
    materialized='incremental',
    unique_key='ba_site_entity_id',
	cluster_by='entity_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    entity_id,
    store_id,
    total_weight,
    total_qty,
    email_sent,
    order_id,
    customer_id,
    shipping_address_id,
    billing_address_id,
    shipment_status,
    increment_id,
    timestamp(created_at) as created_at,
    timestamp(updated_at) as updated_at,
    packages,
    shipping_label,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__sales_flat_shipment') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    entity_id,
    store_id,
    total_weight,
    total_qty,
    email_sent,
    order_id,
    customer_id,
    shipping_address_id,
    billing_address_id,
    shipment_status,
    increment_id,
    created_at,
    updated_at,
    packages,
    shipping_label,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__sales_flat_shipment') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}