{{ config(
    materialized='table'
) }}


select 
    month_start_date,
    month_name,
    department,
    sales_forecast
from {{ source('core', 'stock_sales_forecast_gsheet') }}
