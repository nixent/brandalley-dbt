{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'custom_role'), source('zendesk_fr_5x', 'custom_role')]
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
    configuration_chat_access,
    configuration_end_user_list_access,
    configuration_end_user_profile_access,
    configuration_forum_access,
    configuration_forum_access_restricted_content,
    configuration_group_access,
    configuration_light_agent,
    configuration_macro_access,
    configuration_manage_business_rules,
    configuration_manage_dynamic_content,
    configuration_manage_extensions_and_channels,
    configuration_manage_facebook,
    configuration_moderate_forums,
    configuration_organization_editing,
    configuration_organization_notes_editing,
    configuration_report_access,
    configuration_ticket_access,
    configuration_ticket_comment_access,
    configuration_ticket_deletion,
    configuration_ticket_editing,
    configuration_ticket_merge,
    configuration_ticket_tag_editing,
    configuration_twitter_search_access,
    configuration_user_view_access,
    configuration_view_access,
    configuration_view_deleted_tickets,
    configuration_voice_access,
    created_at,
    description,
    name,
    role_type,
    updated_at
from site_group