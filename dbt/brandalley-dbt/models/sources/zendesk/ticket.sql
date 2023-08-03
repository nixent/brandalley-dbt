{{ config(
    materialized='table',
    schema='zendesk'
) }}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[source('zendesk_uk_5x', 'ticket'), source('zendesk_fr_5x', 'ticket')]
    ) }}
),

site_group as (
    select 
        case when _dbt_source_relation like '%zendesk_uk_5x%' then 'UK' else 'FR' end as ba_site,
        * 
    from unioned
)

select
  ba_site || '-' || id as id,
  _fivetran_synced,
  allow_channelback,
  ba_site || '-' || assignee_id as assignee_id,
  ba_site || '-' || brand_id as brand_id,
  created_at,
  custom_carrier,
  custom_client_contact_reason_no_order,
  custom_client_contact_reason_order,
  custom_contact_reason,
  custom_coupon_code,
  custom_needs_senior_help,
  custom_order_id,
  custom_order_number,
  custom_order_related,
  custom_post_code,
  custom_product_issue_type,
  custom_shipping_issue_type,
  custom_solution,
  ba_site || '-' || custom_status_id as custom_status_id,
  custom_tracking_,
  description,
  due_at,
  external_id,
  followup_ids,
  ba_site || '-' || forum_topic_id as forum_topic_id,
  ba_site || '-' || group_id as group_id,
  has_incidents,
  is_public,
  merged_ticket_ids,
  organization_id,
  priority,
  problem_id,
  recipient,
  ba_site || '-' || requester_id as requester_id,
  status,
  subject,
  ba_site || '-' || submitter_id as submitter_id,
  system_client,
  system_email_id,
  system_eml_redacted,
  system_ip_address,
  system_json_email_identifier,
  system_latitude,
  system_location,
  system_longitude,
  system_machine_generated,
  system_message_id,
  system_raw_email_identifier,
  ticket_form_id,
  type,
  updated_at,
  url,
  via_channel,
  via_followup_source_id,
  via_source_from_address,
  via_source_from_id,
  via_source_from_title,
  via_source_rel,
  via_source_to_address,
  via_source_to_name
from site_group