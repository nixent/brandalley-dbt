{{config(
    materialized='incremental',
    unique_key='stockistid',
	cluster_by='stockistid',
)}}

{{streamkap_incremental_on_source_to_current(source_name='stockist', id_field=config.get('unique_key'))}}