{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_collaborator'), source('zendesk_fr_5x', 'ticket_collaborator')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    ba_site || '-'  || ticket_id as ticket_id,
    ba_site || '-'  || user_id as user_id,
    _fivetran_synced
from site_group