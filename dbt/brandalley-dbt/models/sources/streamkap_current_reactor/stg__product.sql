{{config(
    materialized='incremental',
    unique_key='productid',
	cluster_by='productid',
)}}

{{streamkap_incremental_on_source_to_current(source_name='product', id_field=config.get('unique_key'))}}