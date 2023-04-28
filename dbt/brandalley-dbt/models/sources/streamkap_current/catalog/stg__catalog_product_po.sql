{{config(
    materialized='incremental',
    unique_key='ba_site_po_id',
	cluster_by='ba_site_po_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    po_id,
    negotiation_id,
    created_at,
    updated_at,
    status,
    sap_ref,
    sap_message,
    delivery_date,
    date_exported,
    products_in_wh_b,
    products_export_process_id,
    purchase_order_export_process_id,
    purchase_order_in_wh_b,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__catalog_product_po') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

{# union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    po_id,
    negotiation_id,
    created_at,
    updated_at,
    status,
    sap_ref,
    sap_message,
    delivery_date,
    date_exported,
    products_in_wh_b,
    products_export_process_id,
    purchase_order_export_process_id,
    purchase_order_in_wh_b,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__catalog_product_po') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %} #}