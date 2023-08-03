{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'user'), source('zendesk_fr_5x', 'user')]
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
    alias,
    authenticity_token,
    chat_only,
    created_at,
    custom_agent_s_company,
    custom_don_t_send_csat,
    custom_don_t_send_pending_reminder,
    ba_site || '-'  || custom_role_id as custom_role_id,
    ba_site || '-'  || default_group_id as default_group_id,
    details,
    email,
    external_id,
    iana_time_zone,
    last_login_at,
    locale,
    locale_id,
    moderator,
    name,
    notes,
    only_private_comments,
    ba_site || '-'  || organization_id as organization_id,
    phone,
    remote_photo_url,
    report_csv,
    restricted_agent,
    role,
    shared,
    shared_agent,
    signature,
    suspended,
    ticket_restriction,
    time_zone,
    two_factor_auth_enabled,
    updated_at,
    url,
    verified
from site_group