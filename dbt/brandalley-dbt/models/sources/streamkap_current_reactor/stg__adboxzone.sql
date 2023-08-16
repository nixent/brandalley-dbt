{{config(
    materialized='incremental',
    unique_key='zoneid',
	cluster_by='zoneid',
)}}

{{streamkap_incremental_on_source_to_current(source_name='adboxzone', id_field=config.get('unique_key'))}}