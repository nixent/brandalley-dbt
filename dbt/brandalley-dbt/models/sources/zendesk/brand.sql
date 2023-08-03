{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'brand'), source('zendesk_fr_5x', 'brand')]
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
    active,
    brand_url,
    default,
    has_help_center,
    help_center_state,
    logo_content_type,
    logo_content_url,
    logo_deleted,
    logo_file_name,
    logo_height,
    logo_id,
    logo_inline,
    logo_mapped_content_url,
    logo_size,
    logo_url,
    logo_width,
    name,
    subdomain,
    url
from site_group