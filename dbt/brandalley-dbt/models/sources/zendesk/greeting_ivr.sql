{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

{# with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'greeting_ivr'), source('zendesk_fr_5x', 'greeting_ivr')]
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
    ba_site || '-'  || greeting_id as greeting_id,
    _fivetran_deleted,
    _fivetran_synced,
from site_group #}

select 
  'UK' || '-'  || id as id,
    'UK' || '-'  || greeting_id as greeting_id,
    _fivetran_deleted,
    _fivetran_synced,
from {{ source('zendesk_uk_5x', 'greeting_ivr') }}