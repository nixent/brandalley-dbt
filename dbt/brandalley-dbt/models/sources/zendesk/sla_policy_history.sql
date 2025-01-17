{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'sla_policy_history'), source('zendesk_fr_5x', 'sla_policy_history')]
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
    updated_at,
    _fivetran_deleted,
    _fivetran_synced,
    created_at,
    description,
    position,
    title
from site_group