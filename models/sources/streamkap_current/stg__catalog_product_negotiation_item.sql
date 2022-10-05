{{config(
    materialized='incremental',
    unique_key='negotiation_item_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_negotiation_item', id_field=config.get('unique_key'))}}