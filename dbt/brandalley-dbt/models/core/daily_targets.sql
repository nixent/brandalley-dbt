{{ config(
    materialized='table'
)}}

with daily_dates as (
    select
        date_day
    from unnest(generate_date_array('2023-01-01', '2023-12-31', interval 1 day))                 as date_day
)

select
    dd.date_day,
    round(safe_divide(dst.gmv_target,extract(day from last_day(dd.date_day, month))),2)          as gmv_target,
    round(safe_divide(dst.sales_amount_target,extract(day from last_day(dd.date_day, month))),2) as sales_amount_target,
    round(safe_divide(dst.margin_target,extract(day from last_day(dd.date_day, month))),2)       as margin_target,
    dst.aov_target,
    dst.avg_units_target,
    dst.effective_avg_vat_rate
from {{ ref('daily_sales_targets') }} dst
left join daily_dates dd 
    on dst.month_beginning = date_trunc(dd.date_day, month)
