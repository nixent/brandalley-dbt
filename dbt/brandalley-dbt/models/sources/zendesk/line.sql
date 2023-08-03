{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'line'), source('zendesk_fr_5x', 'line')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    ba_site || '-'  || id as id,
    _fivetran_deleted,
    _fivetran_synced,
    ba_site || '-'  || brand_id as brand_id,
    call_recording_consent,
    capabilities_emergency_address,
    capabilities_mms,
    capabilities_sms,
    capabilities_voice,
    country_code,
    created_at,
    default_group_id,
    display_number,
    external,
    failover_number,
    ba_site || '-'  || ivr_id as ivr_id,
    line_type,
    location,
    name,
    nickname,
    number,
    outbound_enabled,
    outbound_number,
    priority,
    recorded,
    ba_site || '-'  || schedule_id as schedule_id,
    sms_enabled,
    sms_group_id,
    token,
    toll_free,
    transcription,
    voice_enabled
from site_group