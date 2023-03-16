{{ config(
    materialized='incremental',
    unique_key='unique_key'
)}}

select
    {{dbt_utils.surrogate_key(['id', 'anonymous_id'])}} as unique_key,
    channel                                             as platform,
    anonymous_id,
    id                                                  as event_id,
    original_timestamp                                  as event_at,
    timestamp                                           as recalc_event_at,
    context_campaign_name                               as campaign_name,
    context_campaign_source                             as campaign_source,
    context_campaign_medium                             as campaign_medium,
    context_campaign_term                               as campaign_term,
    context_page_url                                    as page_url,
    path                                                as page_path,
    context_page_title                                  as page_title,
    search                                              as search_query,
    referrer                                            as page_referrer,
    context_page_referring_domain                       as page_referrer_domain,
    context_page_initial_referrer                       as initial_page_referrer,
    context_page_initial_referring_domain               as initial_page_referrer_domain,
    sent_at                                             as rs_sent_at,
    loaded_at                                           as rs_loaded_at,
    received_at                                         as rs_received_at,
    context_session_start                               as is_session_start,
    context_session_id                                  as session_id
from {{ source('prod', 'pages') }}
where 1=1
{% if is_incremental() %}
  and loaded_at >= (select max(rs_loaded_at) from {{ this }})
{% endif %}
qualify row_number() over (partition by unique_key order by rs_loaded_at desc) = 1