{{config(
    materialized='incremental',
    unique_key='entity_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='sales_flat_shipment', id_field=config.get('unique_key'))}}