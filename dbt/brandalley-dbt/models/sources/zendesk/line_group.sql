{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

{# with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'line_group'), source('zendesk_fr_5x', 'line_group')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    ba_site || '-'  || group_id as group_id,
    ba_site || '-'  || line_id as line_id,
    _fivetran_deleted,
    _fivetran_synced
from site_group #}

select
    'UK' || '-'  || group_id as group_id,
    'UK' || '-'  || line_id as line_id,
    _fivetran_deleted,
    _fivetran_synced
from {{ source('zendesk_uk_5x', 'line_group') }}