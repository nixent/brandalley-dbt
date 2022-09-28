{{config(
    materialized='incremental',
    unique_by='log_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='log_customer', id_field='log_id')}}