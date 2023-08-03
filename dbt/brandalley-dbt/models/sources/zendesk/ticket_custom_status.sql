{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_custom_status'), source('zendesk_fr_5x', 'ticket_custom_status')]
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
    active,
    agent_label,
    created_at,
    `default`,
    description,
    end_user_description,
    end_user_label,
    status_category,
    ba_site || '-'  || ticket_custom_field_id as ticket_custom_field_id,
    updated_at
from site_group