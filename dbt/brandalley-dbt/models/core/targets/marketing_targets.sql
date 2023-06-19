
{{ config(
    materialized='table'
) }}


select 
    Date                   as target_date,
    'UK'                   as ba_site,
    UK_Orders_RC_Forecast  as returning_customers_order_forecast,
    UK_NC_Forecast         as new_customers_order_forecast,
    UK_Members_Forecast    as new_members_forecast
from {{ source('analytics', 'marketing_targets_gsheet') }}
where Date is not null

union all

select 
    Date                   as target_date,
    'FR'                   as ba_site,
    FR_Orders_RC_Forecast  as returning_customers_order_forecast,
    FR_NC_Forecast         as new_customers_order_forecast,
    FR_Members_Forecast    as new_members_forecast
from {{ source('analytics', 'marketing_targets_gsheet') }}
where Date is not null