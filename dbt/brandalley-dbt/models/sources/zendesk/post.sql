{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'post'), source('zendesk_fr_5x', 'post')]
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
    _fivetran_synced,
    ba_site || '-'  || author_id as author_id,
    closed,
    comment_count,
    created_at,
    details,
    featured,
    follower_count,
    html_url,
    pinned,
    status,
    title,
    ba_site || '-'  || topic_id as topic_id,
    url,
    vote_count,
    vote_sum
from site_group