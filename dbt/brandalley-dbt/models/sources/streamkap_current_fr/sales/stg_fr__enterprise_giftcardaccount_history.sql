{{config(
    materialized='incremental',
    unique_key='history_id',
	cluster_by='history_id',
)}}

select history_id,giftcardaccount_id,timestamp(updated_at) as updated_at,action,balance_amount,balance_delta,additional_info,
_streamkap_source_ts_ms, _streamkap_ts_ms, __deleted, _streamkap_offset, _streamkap_loaded_at_ts from (
{{streamkap_incremental_on_source_to_current(source_name='enterprise_giftcardaccount_history', id_field=config.get('unique_key'))}})