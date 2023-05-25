with order_stats as (
    select
        order_created_at_day,
        ba_site,
        total_order_count,
        total_new_customer_count,
        total_new_members,
        gmv,
        margin,
        qty_ordered
    from {{ ref('kpis_daily')}}
)

select
    d.date_day,
    {# for dev #}
    d.last_year_same_day,
    {# d.last_week, #}
    d.last_year,
    {# d.last_month, #}
    {# end for dev #}
    os.ba_site,
    os.total_order_count,
    os.total_new_customer_count,
    os.total_new_members,
    os.gmv,
    os.margin,
    os.qty_ordered,
    {# os1.total_order_count as last_week_total_order_count,
    os2.total_order_count as last_month_total_order_count, #}
    os3.total_order_count           as last_year_total_order_count,
    os4.total_order_count           as last_year_same_day_total_order_count,
    os3.total_new_customer_count    as last_year_total_new_customer_count,
    os4.total_new_customer_count    as last_year_same_day_total_new_customer_count,
    os3.total_new_members           as last_year_total_new_member_count,
    os4.total_new_members           as last_year_same_day_total_new_member_count,
    os3.gmv                         as last_year_gmv,
    os4.gmv                         as last_year_same_day_gmv,
    os3.margin                      as last_year_margin,
    os4.margin                      as last_year_same_day_margin,
    os3.qty_ordered                 as last_year_qty_ordered,
    os4.qty_ordered                 as last_year_same_day_qty_ordered
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

