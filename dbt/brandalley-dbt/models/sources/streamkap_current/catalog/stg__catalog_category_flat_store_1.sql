{{config(
    materialized='incremental',
    unique_key='entity_id',
	cluster_by='entity_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_category_flat_store_1', id_field=config.get('unique_key'))}}