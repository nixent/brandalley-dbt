{{config(
    materialized='incremental',
    unique_key=['option_id', 'store_id']
)}}

{{streamkap_incremental_on_source_to_current(source_name='eav_attribute_option_value', id_field='option_id, store_id')}}