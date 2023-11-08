{{config(
    materialized='incremental',
    unique_key='order_shipment_unique',
	cluster_by='order_shipment_unique',
    
)}}

{{streamkap_incremental_on_source_to_current(source_name='prism_dispatch_order_shipments', id_field=config.get('unique_key'))}}