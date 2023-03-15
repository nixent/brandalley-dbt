{{config(
    materialized='incremental',
    unique_key=['entity_id','attribute_id'],
	cluster_by=['entity_id','attribute_id'],
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_product_entity_decimal', id_field=config.get('unique_key'))}}