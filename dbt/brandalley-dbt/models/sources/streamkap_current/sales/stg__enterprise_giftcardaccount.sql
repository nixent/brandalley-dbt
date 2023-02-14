{{config(
    materialized='incremental',
    unique_key='giftcardaccount_id',
	cluster_by='giftcardaccount_id',
)}}

select giftcardaccount_id,code,status,date(date_created) date_created,date(date_expires) date_expires,website_id,balance,state,is_redeemable, 
_streamkap_source_ts_ms, _streamkap_ts_ms, __deleted, _streamkap_offset, _streamkap_loaded_at_ts
from (
{{streamkap_incremental_on_source_to_current(source_name='enterprise_giftcardaccount', id_field=config.get('unique_key'))}})