{{config(
    materialized='incremental',
    unique_key='wh_stock_id',
	cluster_by='wh_stock_id',
    
)}}

{{streamkap_incremental_on_source_to_current(source_name='warehouse_stock_running_balance', id_field=config.get('unique_key'))}}