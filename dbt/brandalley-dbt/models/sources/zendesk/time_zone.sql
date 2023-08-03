{{ config(
    materialized='table',
    schema='zendesk'
) }}

select 
    * 
from {{ source('zendesk_uk_5x', 'time_zone') }}