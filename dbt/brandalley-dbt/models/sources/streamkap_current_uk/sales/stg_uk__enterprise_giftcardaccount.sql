{{config(
    materialized='incremental',
    unique_key='giftcardaccount_id',
	cluster_by='giftcardaccount_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='enterprise_giftcardaccount', id_field=config.get('unique_key'))}}