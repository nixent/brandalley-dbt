{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket_comment'), source('zendesk_fr_5x', 'ticket_comment')]
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
    body,
    call_duration,
    call_id,
    created,
    facebook_comment,
    from,
    html_body,
    location,
    public,
    recording_url,
    started_at,
    ba_site || '-'  || ticket_id as ticket_id,
    to,
    transcription_status,
    trusted,
    tweet,
    ba_site || '-'  || user_id as user_id,
    via_channel,
    via_source_from_address,
    via_source_from_id,
    via_source_from_title,
    via_source_rel,
    via_source_to_address,
    via_source_to_name,
    voice_comment,
    voice_comment_transcription_visible
from site_group