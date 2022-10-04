{{config(
    materialized='incremental',
    unique_key='link_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_super_link', id_field='link_id')}}