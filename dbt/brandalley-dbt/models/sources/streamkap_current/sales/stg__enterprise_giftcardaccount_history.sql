{{config(
    materialized='incremental',
    unique_key='ba_site_history_id',
	cluster_by='history_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    history_id,
    giftcardaccount_id,
    timestamp(updated_at) as updated_at,
    action,
    balance_amount,
    balance_delta,
    additional_info,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    __deleted,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    bq_last_processed_at
from {{ ref('stg_uk__enterprise_giftcardaccount_history') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    history_id,
    giftcardaccount_id,
    updated_at,
    action,
    balance_amount,
    balance_delta,
    additional_info,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    __deleted,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    bq_last_processed_at
from {{ ref('stg_fr__enterprise_giftcardaccount_history') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}