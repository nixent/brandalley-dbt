{{config(
    materialized='incremental',
    unique_key='ba_site_rule_id',
	cluster_by='rule_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    rule_id,
    name,
    description,
    timestamp(from_date) as from_date,
    timestamp(to_date) as to_date,
    uses_per_customer,
    is_active,
    conditions_serialized,
    actions_serialized,
    stop_rules_processing,
    is_advanced,
    product_ids,
    sort_order,
    simple_action,
    discount_amount,
    discount_qty,
    discount_step,
    simple_free_shipping,
    apply_to_shipping,
    times_used,
    is_rss,
    coupon_type,
    use_auto_generation,
    uses_per_coupon,
    promo_sku,
    promo_cats,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__salesrule') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    rule_id,
    name,
    description,
    timestamp(null) as from_date,
    timestamp(null) as to_date,
    uses_per_customer,
    is_active,
    conditions_serialized,
    actions_serialized,
    stop_rules_processing,
    is_advanced,
    product_ids,
    sort_order,
    simple_action,
    discount_amount,
    discount_qty,
    discount_step,
    simple_free_shipping,
    apply_to_shipping,
    times_used,
    is_rss,
    coupon_type,
    use_auto_generation,
    uses_per_coupon,
    promo_sku,
    promo_cats,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__salesrule') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}