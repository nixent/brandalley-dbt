{{config(
    materialized='incremental',
    unique_key='grn_id',
	cluster_by='grn_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='stock_prism_grn', id_field=config.get('unique_key'))}}