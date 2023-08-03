{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_field_option'), source('zendesk_fr_5x', 'ticket_field_option')]
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
    value,
    _fivetran_synced,
    `default`,
    name,
    ba_site || '-'  || ticket_custom_field_id as ticket_custom_field_id
from site_group