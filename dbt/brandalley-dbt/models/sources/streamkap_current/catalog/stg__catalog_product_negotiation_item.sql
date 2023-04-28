{{config(
    materialized='incremental',
    unique_key='ba_site_negotiation_item_id',
	cluster_by='ba_site_negotiation_item_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    negotiation_item_id,
    negotiation_id,
    timestamp(updated_at) as updated_at,
    product_id,
    product_parent_id,
    sku,
    parrent_sku,
    reference,
    name,
    size,
    brand,
    commodity_code,
    pack_size,
    cost,
    cost_gbp,
    rrp,
    price,
    tax_rate,
    tax_code,
    qty,
    ordered,
    to_order,
    qty_exported,
    imported_to_erp,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__catalog_product_negotiation_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    negotiation_item_id,
    negotiation_id,
    updated_at,
    product_id,
    product_parent_id,
    sku,
    parrent_sku,
    reference,
    name,
    size,
    brand,
    commodity_code,
    pack_size,
    cost,
    cost_gbp,
    rrp,
    price,
    tax_rate,
    tax_code,
    qty,
    ordered,
    to_order,
    qty_exported,
    imported_to_erp,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__catalog_product_negotiation_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}