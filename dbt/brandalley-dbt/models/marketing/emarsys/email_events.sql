{{ config(
    materialized='table'
)}}

select
    'sent' as action,
    es.contact_id, 
    es.launch_id, 
    es.campaign_type, 
    ecp.name            as campaign_name,
    ecp.category_name   as campaign_category,
    ecp.subject         as campaign_subject,
    es.domain, 
    es.campaign_id, 
    es.message_id, 
    es.event_time, 
    es.event_time as email_sent_at, 
    es.customer_id,
    null as link_id,
    null as category_name,
    null as link_name
from {{ ref('email_sends') }} es
left join {{ ref('email_campaigns_latest_version') }} ecp
    on es.campaign_id = ecp.campaign_id
where date(es.partitiontime) >= '2022-01-01'


union all

select 
    'clicked' as action,
    ec.contact_id,
    ec.launch_id,
    ec.campaign_type,
    ecp.name            as campaign_name,
    ecp.category_name   as campaign_category,
    ecp.subject         as campaign_subject,
    ec.domain,
    ec.campaign_id,
    ec.message_id,
    ec.event_time,
    ec.email_sent_at, 
    ec.customer_id,
    ec.link_id,
    ec.category_name,
    ec.link_name
from {{ ref('email_clicks') }} ec
left join {{ ref('email_campaigns_latest_version') }} ecp
    on ec.campaign_id = ecp.campaign_id
where date(ec.partitiontime) >= '2022-01-01'


union all

select 
    'opened' as action,
    eo.contact_id, 
    eo.launch_id, 
    coalesce(eo.campaign_type, ecp.campaign_type) as campaign_type, 
    ecp.name            as campaign_name,
    ecp.category_name   as campaign_category,
    ecp.subject         as campaign_subject,
    eo.domain, 
    eo.campaign_id, 
    eo.message_id, 
    eo.event_time, 
    eo.email_sent_at, 
    eo.customer_id,
    null as link_id,
    null as category_name,
    null as link_name
from {{ ref('email_opens') }} eo
left join {{ ref('email_campaigns_latest_version') }} ecp
    on eo.campaign_id = ecp.campaign_id
where date(eo.partitiontime) >= '2022-01-01'
