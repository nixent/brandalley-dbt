{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ivr_menu_route'), source('zendesk_fr_5x', 'ivr_menu_route')]
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
    action,
    greeting,
    ba_site || '-'  || ivr_menu_id as ivr_menu_id,
    keypress,
    option_text,
    options_menu_id,
    options_phone_number,
    options_sms_textstring,
    options_textback_phone_number_id
from site_group