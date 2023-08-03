{{ config(
    materialized='table',
    schema='zendesk'
) }}

{# with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ivr_menu'), source('zendesk_fr_5x', 'ivr_menu')]
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
    ba_site || '-'  || ivr_id as ivr_id,
    _fivetran_deleted,
    _fivetran_synced,
    ba_site || '-'  || greeting_id as greeting_id,
    name,
    system_default
from site_group #}

select
'UK' || '-'  || id as id,
    'UK' || '-'  || ivr_id as ivr_id,
    _fivetran_deleted,
    _fivetran_synced,
    'UK' || '-'  || greeting_id as greeting_id,
    name,
    system_default
 from {{ source('zendesk_uk_5x', 'ivr_menu') }}