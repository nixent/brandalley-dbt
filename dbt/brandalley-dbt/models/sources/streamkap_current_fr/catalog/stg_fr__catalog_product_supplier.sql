{{config(
    materialized='incremental',
    unique_key='supplier_id',
	cluster_by='supplier_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_supplier', id_field=config.get('unique_key'))}}