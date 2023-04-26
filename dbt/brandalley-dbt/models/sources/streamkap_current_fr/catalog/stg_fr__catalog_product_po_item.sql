{{config(
    materialized='incremental',
    unique_key='po_item_id',
	cluster_by='po_item_id',
    enabled=false
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_po_item', id_field=config.get('unique_key'))}}