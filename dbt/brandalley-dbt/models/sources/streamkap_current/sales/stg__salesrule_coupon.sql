{{config(
    materialized='incremental',
    unique_key='ba_site_coupon_id',
	cluster_by='coupon_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    coupon_id, 
    rule_id, 
    code, 
    usage_limit, 
    usage_per_customer, 
    times_used, 
    timestamp(expiration_date) as expiration_date, 
    is_primary, 
    timestamp(created_at) as created_at,
    type,
    case 
        when type = 1 then 'Customer Service'
        else 'Marketing'
    end as coupon_type_label,
    _streamkap_source_ts_ms, 
    _streamkap_ts_ms, 
    __deleted, 
    _streamkap_offset, 
    _streamkap_loaded_at_ts,
    bq_last_processed_at
from {{ ref('stg_uk__salesrule_coupon') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    coupon_id, 
    rule_id, 
    code, 
    usage_limit, 
    usage_per_customer, 
    times_used, 
    timestamp(expiration_date) as expiration_date, 
    is_primary, 
    timestamp(created_at) as created_at,
    type,
    case 
        when type = 1 then 'Customer Service'
        else 'Marketing'
    end as coupon_type_label,
    _streamkap_source_ts_ms, 
    _streamkap_ts_ms, 
    __deleted, 
    _streamkap_offset, 
    _streamkap_loaded_at_ts,
    bq_last_processed_at
from {{ ref('stg_fr__salesrule_coupon') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}