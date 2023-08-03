{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'satisfaction_rating'), source('zendesk_fr_5x', 'satisfaction_rating')]
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
    ba_site || '-'  || assignee_id as assignee_id,
    comment,
    created_at,
    ba_site || '-'  || group_id as group_id,
    reason,
    ba_site || '-'  || requester_id as requester_id,
    score,
    ba_site || '-'  || ticket_id as ticket_id,
    updated_at,
    url
from site_group