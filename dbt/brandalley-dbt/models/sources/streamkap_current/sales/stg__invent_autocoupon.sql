{{config(
    materialized='incremental',
    unique_key='id',
	cluster_by='id',
)}}

select 
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
    __deleted, 
    _streamkap_offset, 
    _streamkap_loaded_at_ts
from ({{streamkap_incremental_on_source_to_current(source_name='invent_autocoupon', id_field=config.get('unique_key'))}})