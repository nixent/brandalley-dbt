{{config(
    materialized='incremental',
    unique_key=['order_id', 'sku'],
	cluster_by=['order_id'],
    
)}}

{{streamkap_incremental_on_source_to_current(source_name='warehouse_order_allocation_kettering', id_field=config.get('unique_key'))}}