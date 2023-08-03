{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'agent_condition_required_status'), source('zendesk_fr_5x', 'agent_condition_required_status')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    _fivetran_id,
    _fivetran_deleted,
    _fivetran_synced,
    ba_site || '-'  || agent_condition_id as agent_condition_id,
    status
from site_group