{{ config(
    materialized='table',
    schema='zendesk'
) }}

{# with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ivr'), source('zendesk_fr_5x', 'ivr')]
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
    name
from site_group #}

select 
'UK' || '-'  || id as id,
    _fivetran_deleted,
    _fivetran_synced,
    name
from {{ source('zendesk_uk_5x', 'ivr') }}