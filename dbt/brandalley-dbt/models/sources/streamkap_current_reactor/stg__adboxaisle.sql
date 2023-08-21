{{config(
    materialized='incremental',
    unique_key='aisleid',
	cluster_by='aisleid',
)}}

{{streamkap_incremental_on_source_to_current(source_name='adboxaisle', id_field=config.get('unique_key'))}}