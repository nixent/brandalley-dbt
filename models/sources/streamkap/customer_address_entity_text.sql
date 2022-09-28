{{config(
    materialized='incremental',
    unique_by='value_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='customer_address_entity_text', id_field='value_id')}}