{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_tag'), source('zendesk_fr_5x', 'ticket_tag')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    ba_site || '-'  || tag as tag,
    ba_site || '-'  || ticket_id as ticket_id,
    _fivetran_synced
from site_group