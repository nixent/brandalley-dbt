{{config(
    materialized='incremental',
    unique_key='ba_site_transaction_id',
	cluster_by='transaction_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    transaction_id,
    parent_id,
    order_id,
    payment_id,
    txn_id,
    parent_txn_id,
    txn_type,
    is_closed,
    additional_information,
    timestamp(created_at) as created_at,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__sales_payment_transaction') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    transaction_id,
    parent_id,
    order_id,
    payment_id,
    txn_id,
    parent_txn_id,
    txn_type,
    is_closed,
    additional_information,
    created_at,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__sales_payment_transaction') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}