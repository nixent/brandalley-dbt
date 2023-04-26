{{config(
    materialized='incremental',
    unique_key='po_id',
	cluster_by='po_id',
    enabled=false
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_po', id_field=config.get('unique_key'))}}