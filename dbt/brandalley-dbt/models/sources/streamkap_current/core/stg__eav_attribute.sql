{{ config(
    materialized = 'incremental',
    unique_key = 'ba_site_attribute_id'
) }}

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    attribute_id,
    entity_type_id,
    attribute_code,
    attribute_model,
    backend_model,
    backend_type,
    backend_table,
    frontend_model,
    frontend_input,
    frontend_label,
    frontend_class,
    source_model,
    is_required,
    is_user_defined,
    default_value,
    is_unique,
    note,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_uk__eav_attribute') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    attribute_id,
    entity_type_id,
    attribute_code,
    attribute_model,
    backend_model,
    backend_type,
    backend_table,
    frontend_model,
    frontend_input,
    frontend_label,
    frontend_class,
    source_model,
    is_required,
    is_user_defined,
    default_value,
    is_unique,
    note,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    _streamkap_offset,
    _streamkap_loaded_at_ts,
    __deleted,
    bq_last_processed_at
from {{ ref('stg_fr__eav_attribute') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}