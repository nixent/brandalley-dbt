{{config(
    materialized='incremental',
    unique_key='stock_id',
	cluster_by='stock_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='stock_audit_sellable', id_field=config.get('unique_key'))}}