with order_stats as (
    select
        order_created_at_day,
        ba_site,
        total_order_count,
        total_new_customer_count,
        total_new_members,
        gmv,
        sales_amount,
        margin,
        qty_ordered
    from {{ ref('kpis_daily')}}
),

marketing_targets as (
    select
        target_date,
        new_members_forecast,
        returning_customers_order_forecast + new_customers_order_forecast as all_orders_forecast
    from {{ ref('marketing_targets') }}
),

ga_stats as (
    select 
        ga_session_at_date,
        ga_unique_visits
    from {{ ref('ga_conversion_rate') }}
    where date_aggregation_type = 'day'
)

select
    d.date_day,
    {# for dev purposes #}
    d.last_year_same_day,
    {# d.last_week, #}
    d.last_year,
    {# d.last_month, #}
    {# end for dev #}
    os.ba_site,
    gs.ga_unique_visits,
    os.total_order_count,
    os.total_new_customer_count,
    os.total_new_members,
    os.gmv,
    os.sales_amount,
    os.margin,
    os.qty_ordered,
    gs1.ga_unique_visits            as last_year_same_day_ga_unique_visits,
    gs1.ga_orders                   as last_year_same_day_ga_orders,
    os3.total_order_count           as last_year_total_order_count,
    os4.total_order_count           as last_year_same_day_total_order_count,
    os3.total_new_customer_count    as last_year_total_new_customer_count,
    os4.total_new_customer_count    as last_year_same_day_total_new_customer_count,
    os3.total_new_members           as last_year_total_new_member_count,
    os4.total_new_members           as last_year_same_day_total_new_member_count,
    os3.gmv                         as last_year_gmv,
    os4.gmv                         as last_year_same_day_gmv,
    os3.sales_amount                as last_year_sales_amount,
    os4.sales_amount                as last_year_same_day_sales_amount,
    os3.margin                      as last_year_margin,
    os4.margin                      as last_year_same_day_margin,
    os3.qty_ordered                 as last_year_qty_ordered,
    os4.qty_ordered                 as last_year_same_day_qty_ordered,
    mt.new_members_forecast,
    mt.all_orders_forecast
from {{ ref('dates') }} d
left join order_stats os
    on os.order_created_at_day = d.date_day
{# left join order_stats os1
    on os1.order_created_at_day = d.last_week and os.ba_site = os1.ba_site
left join order_stats os2
    on os2.order_created_at_day = d.last_month and os.ba_site = os2.ba_site #}
left join order_stats os3
    on os3.order_created_at_day = d.last_year and os.ba_site = os3.ba_site
left join order_stats os4
    on os4.order_created_at_day = d.last_year_same_day and os.ba_site = os4.ba_site
left join ga_stats gs 
    on gs.ga_session_at_date = d.date_day and os.ba_site = 'UK'
left join ga_stats gs1
    on gs1.ga_session_at_date = d.last_year_same_day and os.ba_site = 'UK'
left join marketing_targets mt 
    on d.date_day = mt.target_date and os.ba_site = 'UK'

