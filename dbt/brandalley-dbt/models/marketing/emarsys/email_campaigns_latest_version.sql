{{ config(materialized='table') }}

select * from {{ ref('email_campaigns') }}
qualify row_number() over (partition by campaign_id order by event_time) = 1