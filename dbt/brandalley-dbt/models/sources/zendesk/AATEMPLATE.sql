{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'end_user_condition'), source('zendesk_fr_5x', 'end_user_condition')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    {{ dbt_utils.star(
        source('zendesk_uk_5x', 'end_user_condition'),
        quote_identifiers=false
    ) }}
    from {{ source('zendesk_uk_5x', 'end_user_condition') }}

{# select
    ba_site || '-'  || id as id,
from site_group #}