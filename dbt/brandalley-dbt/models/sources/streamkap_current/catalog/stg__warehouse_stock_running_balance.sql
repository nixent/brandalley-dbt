{{config(
    materialized='incremental',
    unique_key='ba_site_wh_stock_id'
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    wh_stock_id,
    cast(sku as string) as sku,
    qty_stock_received_prism,
    qty_stock_received_sed,
    qty_stock_received_kettering,
    qty_allocated_prism,
    qty_allocated_sed,
    qty_allocated_kettering,
    qty_remaining_prism,
    qty_remaining_sed,
    qty_remaining_kettering,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__warehouse_stock_running_balance') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

{# union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    wh_stock_id,
    sku,
    qty_stock_received_prism,
    qty_stock_received_sed,
    qty_stock_received_kettering,
    qty_allocated_prism,
    qty_allocated_sed,
    qty_allocated_kettering,
    qty_remaining_prism,
    qty_remaining_sed,
    qty_remaining_kettering,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__eav_attribute_option_value') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %} #}