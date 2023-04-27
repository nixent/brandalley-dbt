{{config(
    materialized='incremental',
    unique_key='ba_site_id',
	cluster_by='id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    id,
    referral_coupon,
    referee_coupon,
    reward_to,
    reward_from,
    timestamp(created_at) as created_at,
    timestamp(rem_email) as rem_email,
    cartrule_id,
    basis_for_issue,
    comments_text,
    recommendation,
    referral_couponid,
    referee_couponid,
    reward_from_old,
    issuer_id,
    timestamp(rem_lastday_email) as rem_lastday_email,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__invent_autocoupon') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    id,
    referral_coupon,
    referee_coupon,
    reward_to,
    reward_from,
    created_at,
    timestamp(null) as rem_email,
    cartrule_id,
    basis_for_issue,
    comments_text,
    recommendation,
    referral_couponid,
    referee_couponid,
    reward_from_old,
    issuer_id,
    timestamp(null) as rem_lastday_email,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__invent_autocoupon') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}