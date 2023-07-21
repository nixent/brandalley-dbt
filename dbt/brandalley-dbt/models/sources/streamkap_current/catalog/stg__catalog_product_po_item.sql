{{config(
    materialized='incremental',
    unique_key='ba_site_po_item_id',
	cluster_by='ba_site_po_item_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    po_item_id,
    po_id,
    timestamp(updated_at) as updated_at,
    product_id,
    configurable_product_id,
    sku,
    configurable_sku,
    reference,
    simple_name,
    configurable_name,
    brand,
    colour,
    web_size,
    supplier_size,
    cost,
    cost_gbp,
    rrp,
    price,
    tax_rate,
    tax_code,
    barcode,
    commodity_code,
    category,
    sub_category,
    sub_sub_category,
    image,
    pack_size,
    to_order,
    to_order_original,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__catalog_product_po_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    po_item_id,
    po_id,
    updated_at,
    product_id,
    configurable_product_id,
    sku,
    configurable_sku,
    reference,
    simple_name,
    configurable_name,
    brand,
    colour,
    web_size,
    supplier_size,
    cost,
    cost_gbp,
    rrp,
    price,
    tax_rate,
    tax_code,
    barcode,
    commodity_code,
    category,
    sub_category,
    sub_sub_category,
    image,
    pack_size,
    to_order,
    to_order_original,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__catalog_product_po_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}