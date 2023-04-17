{{config(
    materialized='incremental',
    unique_key='ba_site_giftcardaccount_id',
	cluster_by='giftcardaccount_id',
)}}

-- TODO after streamkap fix

{# select giftcardaccount_id,code,status,date(date_created) date_created,date(date_expires) date_expires,website_id,balance,state,is_redeemable, 
_streamkap_source_ts_ms, _streamkap_ts_ms, __deleted, _streamkap_offset, _streamkap_loaded_at_ts #}

with unioned as (
    select
    {{ dbt_utils.star(
        ref('stg_uk__enterprise_giftcardaccount'),
        quote_identifiers=false
    ) }}
    from {{ ref('stg_uk__enterprise_giftcardaccount') }}
)

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
from {{ ref('stg_uk__enterprise_giftcardaccount') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
from {{ ref('stg_fr__enterprise_giftcardaccount') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}