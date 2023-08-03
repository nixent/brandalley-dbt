{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'agent_condition'), source('zendesk_fr_5x', 'agent_condition')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
  _fivetran_id,
  _fivetran_deleted,
  _fivetran_synced,
  ba_site || '-' || child_field_id as child_field_id,
  is_required,
  ba_site || '-' || parent_field_id as parent_field_id,
  parent_field_type,
  required_on_statuses_type,
  ba_site || '-' || ticket_form_id as ticket_form_id,
  value
from site_group