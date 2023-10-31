select 
    a.date_day,
    'Current' as period_type,
    a.gmv,
    a.sales_amount,
    a.margin
from {{ ref('daily_sales') }} a

union all   

select 
    a.date_day,
    'Target' as period_type,
    a.gmv_target,
    a.sales_amount_target,
    a.margin_target
from {{ ref('daily_sales') }} a

union all   

select 
    a.date_day,
    'LY' as period_type,
    a.last_year_same_day_gmv,
    a.last_year_same_day_sales_amount,
    a.last_year_same_day_margin
from {{ ref('daily_sales') }} a
