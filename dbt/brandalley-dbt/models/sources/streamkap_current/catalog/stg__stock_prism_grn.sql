{{config(
    materialized='incremental',
    unique_key='ba_site_grn_id',
	cluster_by='ba_site_grn_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    grn_id,
    purchase_order_reference,
    purchase_order_instance,
    date,
    time,
    magento_process_id,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__stock_prism_grn') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    grn_id,
    purchase_order_reference,
    purchase_order_instance,
    date('1970-01-01') + date as date,
    extract(time from timestamp_micros(unix_micros(timestamp(date('1970-01-01') + date)) + time)) as time,
    magento_process_id,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__stock_prism_grn') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}