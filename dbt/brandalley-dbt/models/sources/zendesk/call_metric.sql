{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'call_metric'), source('zendesk_fr_5x', 'call_metric')]
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
    ba_site || '-'  || agent_id as agent_id,
    call_charge,
    call_group_id,
    call_recording_consent,
    call_recording_consent_action,
    call_recording_consent_keypress,
    callback,
    callback_source,
    completion_status,
    consultation_time,
    created_at,
    customer_requested_voicemail,
    default_group,
    direction,
    duration,
    exceeded_queue_wait_time,
    hold_time,
    ivr_action,
    ivr_destination_group_name,
    ivr_hops,
    ivr_routed_to,
    ivr_time_spent,
    minutes_billed,
    not_recording_time,
    outside_business_hours,
    overflowed,
    overflowed_to,
    phone_number,
    phone_number_id,
    quality_issues,
    recording_control_interactions,
    recording_time,
    talk_time,
    ba_site || '-'  || ticket_id as ticket_id,
    time_to_answer,
    updated_at,
    voicemail,
    wait_time,
    wrap_up_time
from site_group