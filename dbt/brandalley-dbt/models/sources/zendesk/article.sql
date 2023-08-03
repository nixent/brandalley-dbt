{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'article'), source('zendesk_fr_5x', 'article')]
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
    _fivetran_synced,
    ba_site || '-'  || author_id as author_id,
    body,
    comments_disabled,
    created_at,
    draft,
    edited_at,
    html_url,
    locale,
    name,
    outdated,
    ba_site || '-'  || permission_group_id as permission_group_id,
    position,
    promoted,
    section_id,
    source_locale,
    title,
    updated_at,
    url,
    vote_count,
    vote_sum
from site_group