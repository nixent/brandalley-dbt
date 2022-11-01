{{config(
    materialized='incremental',
    unique_key='line_id',
	cluster_by='line_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='customer_entity_extra', id_field=config.get('unique_key'))}}