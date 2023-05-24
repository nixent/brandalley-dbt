with order_stats as (
    select
        date_trunc(datetime(o.created_at, "Europe/London"), day)                           as order_created_at_day,
        o.ba_site,
        {# ce.customer_type, #}
        count(distinct o.increment_id)                                                  as total_order_count--,
        {# count(distinct if(o.orderno = 1, o.increment_id, null))                         as total_new_order_count,
        count(distinct if(o.orderno = 1, o.customer_id, null))                          as total_new_customer_count,
        count(distinct if(o.orderno > 1, o.customer_id, null))                          as total_existing_customer_count,
        sum(o.shipping_incl_tax)                                                        as shipping_amount #}
    from {{ ref('Orders')}} o
    left join {{ ref('customers_enriched') }} ce
        on o.customer_id = ce.customer_id
    group by 1,2--,3
)

select
    d.date_day,
    os.ba_site,
    os.total_order_count,
    os1.total_order_count as last_week_total_order_count,
    os2.total_order_count as last_month_total_order_count,
    os3.total_order_count as last_year_total_order_count
from {{ ref('dates') }} d
left join order_stats os
    on os.order_created_at_day = d.date_day
left join order_stats os1
    on os1.order_created_at_day = d.last_week and os.ba_site = os1.ba_site
left join order_stats os2
    on os2.order_created_at_day = d.last_month and os.ba_site = os2.ba_site
left join order_stats os3
    on os3.order_created_at_day = d.last_year and os.ba_site = os3.ba_site

