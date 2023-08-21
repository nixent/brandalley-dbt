{{config(
    materialized='incremental',
    unique_key='customerid',
	cluster_by='customerid',
)}}

{{streamkap_incremental_on_source_to_current(source_name='customers', id_field=config.get('unique_key'))}}