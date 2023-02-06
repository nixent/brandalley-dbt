{{config(
    materialized='incremental',
    unique_key='coupon_id',
	cluster_by='coupon_id',
)}}

select coupon_id, rule_id, code, usage_limit, usage_per_customer, times_used, timestamp(expiration_date) expiration_date, is_primary, timestamp(created_at) created_at,
type, _streamkap_source_ts_ms, _streamkap_ts_ms, __deleted, _streamkap_offset, _streamkap_loaded_at_ts from
{{streamkap_incremental_on_source_to_current(source_name='salesrule_coupon', id_field=config.get('unique_key'))}}