{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'call_leg'), source('zendesk_fr_5x', 'call_leg')]
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
    _fivetran_synced,
    ba_site || '-'  || agent_id as agent_id,
    available_via,
    call_charge,
    ba_site || '-'  || call_id as call_id,
    completion_status,
    conference_from,
    conference_time,
    conference_to,
    consultation_from,
    consultation_time,
    consultation_to,
    created_at,
    duration,
    forwarded_to,
    hold_time,
    minutes_billed,
    talk_time,
    transferred_from,
    transferred_to,
    type,
    updated_at,
    ba_site || '-'  || user_id as user_id,
    wrap_up_time
from site_group