{{config(
    materialized='incremental',
    unique_key='rule_id',
	cluster_by='rule_id',
)}}


{{streamkap_incremental_on_source_to_current(source_name='salesrule', id_field=config.get('unique_key'))}}