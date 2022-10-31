{{config(
    materialized='incremental',
    unique_key='link_id',
	cluster_by='link_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_super_link', id_field=config.get('unique_key'))}}