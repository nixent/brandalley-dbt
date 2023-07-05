{{config(
    materialized='incremental',
    unique_key='ba_site_id',
	cluster_by='ba_site_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    id,
    grn_id,
    grn_item_id,
    unique_reference,
    in_sap,
    sku,
    delivery_number,
    delivery_date,
    stock_type_ss,
    stock_type_qc,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__stock_prism_grn_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    id,
    grn_id,
    grn_item_id,
    unique_reference,
    in_sap,
    sku,
    delivery_number,
    date('1970-01-01') + date as delivery_date,
    stock_type_ss,
    stock_type_qc,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__stock_prism_grn_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}