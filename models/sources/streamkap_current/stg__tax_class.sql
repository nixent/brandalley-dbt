{{config(
    materialized='incremental',
    unique_key='class_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='tax_class', id_field='class_id')}}