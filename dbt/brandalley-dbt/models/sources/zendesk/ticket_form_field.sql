{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_form_field'), source('zendesk_fr_5x', 'ticket_form_field')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
    ba_site || '-'  || ticket_field_id as ticket_field_id,
    ba_site || '-'  || ticket_form_id as ticket_form_id,
    _fivetran_deleted,
    _fivetran_synced
from site_group