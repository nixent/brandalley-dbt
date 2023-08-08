{{config(
    materialized='incremental',
    unique_key='id',
	cluster_by='id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='deliveryconsignment', id_field=config.get('unique_key'))}}