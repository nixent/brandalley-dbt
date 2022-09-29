{{config(
    materialized='incremental',
    unique_by='entity_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='customer_entity', id_field='entity_id')}}