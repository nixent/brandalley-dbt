{{config(
    materialized='incremental',
    unique_by='class_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='tax_class', id_field='class_id')}}