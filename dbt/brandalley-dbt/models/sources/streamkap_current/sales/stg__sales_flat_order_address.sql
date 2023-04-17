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
    customer_address_id,
    quote_address_id,
    region_id,
    customer_id,
    fax,
    region,
    postcode,
    lastname,
    street,
    city,
    email,
    telephone,
    country_id,
    firstname,
    address_type,
    prefix,
    middlename,
    suffix,
    company,
    vat_id,
    vat_is_valid,
    vat_request_id,
    vat_request_date,
    vat_request_success,
    giftregistry_item_id,
    changed,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__sales_flat_order_address') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    entity_id,
    parent_id,
    customer_address_id,
    quote_address_id,
    region_id,
    customer_id,
    fax,
    region,
    postcode,
    lastname,
    street,
    city,
    email,
    telephone,
    country_id,
    firstname,
    address_type,
    prefix,
    middlename,
    suffix,
    company,
    vat_id,
    vat_is_valid,
    vat_request_id,
    vat_request_date,
    vat_request_success,
    giftregistry_item_id,
    changed,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__sales_flat_order_address') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}