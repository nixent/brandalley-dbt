{{config(
    materialized='incremental',
    unique_by='value_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='customer_entity_datetime', id_field='value_id')}}