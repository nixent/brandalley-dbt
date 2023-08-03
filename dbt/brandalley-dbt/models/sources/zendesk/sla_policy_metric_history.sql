{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'sla_policy_metric_history'), source('zendesk_fr_5x', 'sla_policy_metric_history')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    ba_site || '-'  || sla_policy_id as sla_policy_id,
    index,
    sla_policy_updated_at,
    _fivetran_synced,
    business_hours,
    metric,
    priority,
    target
from site_group