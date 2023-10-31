
{{ config(
    materialized='table'
) }}


select 
    target_date,
    'UK'                                    as ba_site,
    uk_returning_customers_order_forecast   as returning_customers_order_forecast,
    uk_new_customers_order_forecast         as new_customers_order_forecast,
    uk_new_members_forecast                 as new_members_forecast
from {{ source('analytics', 'marketing_targets_gsheet') }}
where target_date is not null

union all

select 
    target_date,
    'FR'                                    as ba_site,
    fr_returning_customers_order_forecast   as returning_customers_order_forecast,
    fr_new_customers_order_forecast         as new_customers_order_forecast,
    fr_new_members_forecast                 as new_members_forecast
from {{ source('analytics', 'marketing_targets_gsheet') }}
where target_date is not null