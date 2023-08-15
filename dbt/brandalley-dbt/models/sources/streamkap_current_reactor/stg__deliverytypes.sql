{{config(
    materialized='incremental',
    unique_key='code',
	cluster_by='code',
)}}

{{streamkap_incremental_on_source_to_current(source_name='deliverytypes', id_field=config.get('unique_key'))}}