{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_macro_reference'), source('zendesk_fr_5x', 'ticket_macro_reference')]
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
    created,
    macro_deleted,
    macro_id,
    macro_title,
    ba_site || '-'  || ticket_id as ticket_id,
    ba_site || '-'  || user_id as user_id,
    via_channel,
    via_source_from_address,
    via_source_from_id,
    via_source_from_title,
    via_source_rel,
    via_source_to_address,
    via_source_to_name
from site_group