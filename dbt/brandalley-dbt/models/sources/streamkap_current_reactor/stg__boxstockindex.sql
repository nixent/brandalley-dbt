{{config(
    materialized='incremental',
    unique_key=['boxid','stockid'],
	cluster_by=['boxid','stockid'],
)}}

{{streamkap_incremental_on_source_to_current(source_name='boxstockindex', id_field=config.get('unique_key'))}}