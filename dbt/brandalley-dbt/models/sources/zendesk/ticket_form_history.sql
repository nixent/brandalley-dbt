{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_form_history'), source('zendesk_fr_5x', 'ticket_form_history')]
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
    updated_at,
    _fivetran_deleted,
    _fivetran_synced,
    active,
    created_at,
    default,
    display_name,
    end_user_visible,
    in_all_brands,
    name,
    position,
    raw_display_name,
    raw_name,
    url
from site_group