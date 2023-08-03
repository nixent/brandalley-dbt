{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'greeting'), source('zendesk_fr_5x', 'greeting')]
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
    audio_name,
    audio_url,
    ba_site || '-'  || category_id as category_id,
    default_lang,
    has_sub_settings,
    name,
    pending,
    system_default,
    upload_id
from site_group