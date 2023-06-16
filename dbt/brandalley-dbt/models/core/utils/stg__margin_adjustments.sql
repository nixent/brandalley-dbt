{{ config(
    materialized='table'
)}}

select
    Date           as date,
    UK_Amount      as uk_amount,
    FR_Amount      as fr_amount
from {{ source('analytics', 'margin_adjustments_gsheet') }}
