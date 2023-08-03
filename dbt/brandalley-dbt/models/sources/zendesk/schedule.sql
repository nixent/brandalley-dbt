{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'schedule'), source('zendesk_fr_5x', 'schedule')]
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
    end_time,
    start_time,
    _fivetran_deleted,
    _fivetran_synced,
    created_at,
    end_time_utc,
    name,
    start_time_utc,
    time_zone
from site_group