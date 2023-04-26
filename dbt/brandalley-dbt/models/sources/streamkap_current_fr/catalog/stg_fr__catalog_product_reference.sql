{{config(
    materialized='incremental',
    unique_key='reference_id',
	cluster_by='reference_id',
    enabled=false
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_reference', id_field=config.get('unique_key'))}}