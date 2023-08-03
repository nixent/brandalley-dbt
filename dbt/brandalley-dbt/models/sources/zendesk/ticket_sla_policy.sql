{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_sla_policy'), source('zendesk_fr_5x', 'ticket_sla_policy')]
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
    ba_site || '-'  || ticket_id as ticket_id,
    _fivetran_synced,
    policy_applied_at
from site_group