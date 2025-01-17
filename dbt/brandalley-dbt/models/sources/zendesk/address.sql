{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'address'), source('zendesk_fr_5x', 'address')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
  ba_site || '-' || id as id,
  _fivetran_deleted,
  _fivetran_synced,
  city,
  country_code,
  name,
  provider_reference,
  province,
  state,
  street,
  zip
from site_group