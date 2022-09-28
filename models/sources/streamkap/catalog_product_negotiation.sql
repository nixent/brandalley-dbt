{{config(
    materialized='incremental',
    unique_by='negotiation_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_negotiation', id_field='negotiation_id')}}