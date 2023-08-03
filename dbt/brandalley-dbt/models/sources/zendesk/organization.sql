{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'organization'), source('zendesk_fr_5x', 'organization')]
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
    created_at,
    custom_don_t_send_csat,
    custom_don_t_send_pending_reminder,
    details,
    external_id,
    ba_site || '-'  || group_id as group_id,
    name,
    notes,
    shared_comments,
    shared_tickets,
    updated_at,
    url
from site_group