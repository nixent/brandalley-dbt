{{config(
    materialized='incremental',
    unique_key='ba_site_line_id',
	cluster_by='ba_site_line_id',
)}}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    line_id,
    customer_id,
    number_transactions,
    ltv,
    ltvr,
    timestamp(first_purchase) as first_purchase,
    timestamp(last_purchase) as last_purchase,
    timestamp(first_visit_at) as first_visit_at,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__customer_entity_extra') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    line_id,
    customer_id,
    number_transactions,
    ltv,
    ltvr,
    first_purchase,
    last_purchase,
    first_visit_at,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__customer_entity_extra') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}