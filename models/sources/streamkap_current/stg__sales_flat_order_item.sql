{{config(
    materialized='incremental',
    unique_by='item_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='sales_flat_order_item', id_field='item_id')}}