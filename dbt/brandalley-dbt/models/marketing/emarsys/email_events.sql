{{ config(
    materialized='incremental'
)}}

select
    'sent' as action,
    es.contact_id, 
    es.launch_id, 
    es.campaign_type, 
    ecp.name            as campaign_name,
    case 
        when lower(ecp.name) like '%am%' then 'AM'
        when lower(ecp.name) like '%pm%' then 'PM'
        else 'Life Cycle'
    end as email_type,
    ecp.category_name   as campaign_category,
    ecp.subject         as campaign_subject,
    es.domain, 
    es.campaign_id, 
    es.message_id, 
    es.event_time, 
    es.event_time as email_sent_at, 
    es.customer_id,
    es.partitiontime,
    null as link_id,
    null as category_name,
    null as link_name
from {{ ref('email_sends') }} es
left join {{ ref('email_campaigns_latest_version') }} ecp
    on es.campaign_id = ecp.campaign_id
where date(es.partitiontime) >= '2023-01-01'
{% if is_incremental() %}
    and date(es.partitiontime) > (select max(partitiontime) from {{ this }} where action = 'sent' )
{% endif %}


union all

select 
    'clicked' as action,
    ec.contact_id,
    ec.launch_id,
    ec.campaign_type,
    ecp.name            as campaign_name,
    case 
        when lower(ecp.name) like '%am%' then 'AM'
        when lower(ecp.name) like '%pm%' then 'PM'
        else 'Life Cycle'
    end as email_type,
    ecp.category_name   as campaign_category,
    ecp.subject         as campaign_subject,
    ec.domain,
    ec.campaign_id,
    ec.message_id,
    ec.event_time,
    ec.email_sent_at, 
    ec.customer_id,
    ec.partitiontime,
    ec.link_id,
    ec.category_name,
    ec.link_name
from {{ ref('email_clicks') }} ec
left join {{ ref('email_campaigns_latest_version') }} ecp
    on ec.campaign_id = ecp.campaign_id
where date(ec.partitiontime) >= '2023-01-01'
{% if is_incremental() %}
    and date(ec.partitiontime) > (select max(partitiontime) from {{ this }} where action = 'clicked' )
{% endif %}


union all

select 
    'opened' as action,
    eo.contact_id, 
    eo.launch_id, 
    coalesce(eo.campaign_type, ecp.campaign_type) as campaign_type, 
    ecp.name            as campaign_name,
    case 
        when lower(ecp.name) like '%am%' then 'AM'
        when lower(ecp.name) like '%pm%' then 'PM'
        else 'Life Cycle'
    end as email_type,
    ecp.category_name   as campaign_category,
    ecp.subject         as campaign_subject,
    eo.domain, 
    eo.campaign_id, 
    eo.message_id, 
    eo.event_time, 
    eo.email_sent_at, 
    eo.customer_id,
    eo.partitiontime,
    null as link_id,
    null as category_name,
    null as link_name
from {{ ref('email_opens') }} eo
left join {{ ref('email_campaigns_latest_version') }} ecp
    on eo.campaign_id = ecp.campaign_id
where date(eo.partitiontime) >= '2023-01-01'
{% if is_incremental() %}
    and date(eo.partitiontime) > (select max(partitiontime) from {{ this }} where action = 'opened' )
{% endif %}

union all

select 
    'bounced' as action,
    eb.contact_id, 
    eb.launch_id, 
    coalesce(eb.campaign_type, ecp.campaign_type) as campaign_type, 
    ecp.name            as campaign_name,
    case 
        when lower(ecp.name) like '%am%' then 'AM'
        when lower(ecp.name) like '%pm%' then 'PM'
        else 'Life Cycle'
    end as email_type,
    ecp.category_name   as campaign_category,
    ecp.subject         as campaign_subject,
    eb.domain, 
    eb.campaign_id, 
    eb.message_id, 
    eb.event_time, 
    eb.email_sent_at, 
    eb.customer_id,
    eb.partitiontime,
    null as link_id,
    null as category_name,
    null as link_name
from {{ ref('email_bounces') }} eb
left join {{ ref('email_campaigns_latest_version') }} ecp
    on eb.campaign_id = ecp.campaign_id
where date(eb.partitiontime) >= '2023-01-01'
{% if is_incremental() %}
    and date(eb.partitiontime) > (select max(partitiontime) from {{ this }} where action = 'bounced' )
{% endif %}
