{{config(
    materialized='incremental',
    unique_key='ba_site_entity_id',
	cluster_by='entity_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    entity_id,
    parent_id,
    base_shipping_captured,
    shipping_captured,
    amount_refunded,
    base_amount_paid,
    amount_canceled,
    base_amount_authorized,
    base_amount_paid_online,
    base_amount_refunded_online,
    base_shipping_amount,
    shipping_amount,
    amount_paid,
    amount_authorized,
    base_amount_ordered,
    base_shipping_refunded,
    shipping_refunded,
    base_amount_refunded,
    amount_ordered,
    base_amount_canceled,
    quote_payment_id,
    additional_data,
    cc_exp_month,
    cc_ss_start_year,
    echeck_bank_name,
    method,
    cc_debug_request_body,
    cc_secure_verify,
    protection_eligibility,
    cc_approval,
    cc_last4,
    cc_status_description,
    echeck_type,
    cc_debug_response_serialized,
    cc_ss_start_month,
    echeck_account_type,
    last_trans_id,
    cc_cid_status,
    cc_owner,
    cc_type,
    po_number,
    cc_exp_year,
    cc_status,
    echeck_routing_number,
    account_status,
    anet_trans_method,
    cc_debug_response_body,
    cc_ss_issue,
    echeck_account_name,
    cc_avs_status,
    cc_number_enc,
    cc_trans_id,
    paybox_request_number,
    address_status,
    additional_information,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    clearpay_token,
    clearpay_order_id,
    timestamp(clearpay_fetched_at) as clearpay_fetched_at,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__sales_flat_order_payment') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    entity_id,
    parent_id,
    base_shipping_captured,
    shipping_captured,
    amount_refunded,
    base_amount_paid,
    amount_canceled,
    base_amount_authorized,
    base_amount_paid_online,
    base_amount_refunded_online,
    base_shipping_amount,
    shipping_amount,
    amount_paid,
    amount_authorized,
    base_amount_ordered,
    base_shipping_refunded,
    shipping_refunded,
    base_amount_refunded,
    amount_ordered,
    base_amount_canceled,
    quote_payment_id,
    additional_data,
    cc_exp_month,
    cc_ss_start_year,
    echeck_bank_name,
    method,
    cc_debug_request_body,
    cc_secure_verify,
    protection_eligibility,
    cc_approval,
    cc_last4,
    cc_status_description,
    echeck_type,
    cc_debug_response_serialized,
    cc_ss_start_month,
    echeck_account_type,
    last_trans_id,
    cc_cid_status,
    cc_owner,
    cc_type,
    po_number,
    cc_exp_year,
    cc_status,
    echeck_routing_number,
    account_status,
    anet_trans_method,
    cc_debug_response_body,
    cc_ss_issue,
    echeck_account_name,
    cc_avs_status,
    cc_number_enc,
    cc_trans_id,
    paybox_request_number,
    address_status,
    additional_information,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    clearpay_token,
    clearpay_order_id,
    clearpay_fetched_at,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__sales_flat_order_payment') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}