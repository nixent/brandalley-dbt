{{ config(
    materialized='table',
    schema='zendesk_5x'
) }}

select 
    * 
from {{ source('zendesk_uk_5x', 'time_zone') }}