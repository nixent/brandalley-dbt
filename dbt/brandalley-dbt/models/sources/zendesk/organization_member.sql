{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'organization_member'), source('zendesk_fr_5x', 'organization_member')]
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
    ba_site || '-'  || organization_id as organization_id,
    ba_site || '-'  || user_id as user_id
from site_group