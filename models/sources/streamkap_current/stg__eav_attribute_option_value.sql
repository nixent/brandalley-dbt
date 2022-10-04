{{config(
    materialized='incremental',
    unique_key='value_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='eav_attribute_option_value', id_field='value_id')}}