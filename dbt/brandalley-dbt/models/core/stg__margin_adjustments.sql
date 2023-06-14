{{ config(
    materialized='table'
)}}

select
    Date           as date,
    Amount         as amount
from {{ source('analytics', 'margin_adjustments_gsheet') }}
