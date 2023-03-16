{{ config(
    enabled=false
)}}

select
    channel as platform,
    anonymous_id,
    context_campaign_name as campaign_name,
    context_page_url as page_url,
    original_timestamp as event_at,
    url,
    context_campaign_source as campaign_source,
    context_campaign_term as campaign_term,
    sent_at as rs_sent_at,
    timestamp as recalc_event_at,
    referrer as page_referrer,
    context_page_initial_referrer as initial_page_referrer,
    context_page_initial_referring_domain as initial_page_referrer_domain,
    context_campaign_medium,
    path as page_path,
    uuid_ts,
    title,
    context_page_referring_domain as page_referrer_domain,
    context_page_title as page_title,
    id,
    search as search_query,
    loaded_at as rs_loaded_at,
    context_session_start as is_session_start,
    context_session_id as session_id
from {{ source('prod', 'pages') }}