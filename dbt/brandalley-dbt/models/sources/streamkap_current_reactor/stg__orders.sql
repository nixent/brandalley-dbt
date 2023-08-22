{{config(
    materialized='incremental',
    unique_key='orderid',
	cluster_by='orderid',
)}}

{{streamkap_incremental_on_source_to_current(source_name='orders', id_field=config.get('unique_key'))}}