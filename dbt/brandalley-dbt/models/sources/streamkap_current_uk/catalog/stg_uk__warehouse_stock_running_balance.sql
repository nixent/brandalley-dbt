{{config(
    materialized='incremental',
    unique_key='sku',
	cluster_by='sku',
    
)}}

{{streamkap_incremental_on_source_to_current(source_name='warehouse_stock_running_balance', id_field=config.get('unique_key'))}}