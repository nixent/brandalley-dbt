{{config(
    materialized='incremental',
    unique_key='subscriber_id',
	cluster_by='subscriber_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='newsletter_subscriber', id_field=config.get('unique_key'))}}