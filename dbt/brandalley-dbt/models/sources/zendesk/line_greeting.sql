{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'line_greeting'), source('zendesk_fr_5x', 'line_greeting')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    ba_site || '-'  || greeting_id as greeting_id,
    ba_site || '-'  || line_id as line_id,
    _fivetran_deleted,
    _fivetran_synced,
    is_default
from site_group