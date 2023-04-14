{{config(
    materialized='incremental',
    unique_key= 'ba_site_user_id'
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    user_id,
    firstname,
    lastname,
    email,
    username,
    password,
    created,
    modified,
    logdate,
    lognum,
    reload_acl_flag,
    is_active,
    extra,
    rp_token,
    rp_token_created_at,
    failures_num,
    first_failure,
    lock_expires,
    token_login_enabled,
    login_token_secret,
    last_token_used,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__admin_user') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    user_id,
    firstname,
    lastname,
    email,
    username,
    password,
    cast(created as string),
    cast(modified as string),
    cast(logdate as string),
    lognum,
    reload_acl_flag,
    is_active,
    extra,
    rp_token,
    cast(rp_token_created_at as string),
    failures_num,
    cast(first_failure as string),
    cast(lock_expires as string),
    token_login_enabled,
    login_token_secret,
    last_token_used,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__admin_user') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}
