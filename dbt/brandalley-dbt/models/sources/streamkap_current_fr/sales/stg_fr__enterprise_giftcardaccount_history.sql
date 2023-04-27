{{config(
    materialized='incremental',
    unique_key='history_id',
	cluster_by='history_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='enterprise_giftcardaccount_history', id_field=config.get('unique_key'))}}