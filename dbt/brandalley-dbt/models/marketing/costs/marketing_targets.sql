
{{ config(
    materialized='table'
) }}


select 
    Date                as target_date,
    Orders_RC_Forecast  as returning_customers_order_forecast,
    NC_Forecast         as new_customers_order_forecast,
    Members_Forecast    as new_members_forecast
from {{ source('analytics', 'marketing_targets_gsheet') }}
where Date is not null