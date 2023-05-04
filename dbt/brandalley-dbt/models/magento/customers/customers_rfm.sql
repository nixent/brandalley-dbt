with orderlines_agg as (
    select 
        customer_id, 
        ba_site,
        date_diff(current_date(), max(date(order_placed_date)), day)    as days_since_last_order,
        count(distinct order_id)                                        as lifetime_orders, 
        round(sum(total_local_currency_ex_tax_after_vouchers),0)        as lifetime_sales_amount,
        round(sum(margin),0)                                            as lifetime_margin
    from {{ ref('OrderLines') }}
    where customer_id is not null 
    group by 1,2
),

customer_lifetime_stats as (
    select 
        ola.customer_id, 
        ola.ba_site,
        ola.days_since_last_order, 
        ola.lifetime_orders, 
        ola.lifetime_sales_amount,
        ola.lifetime_margin,
        safe_cast(round(avg(o.interval_between_orders),0) as integer) as avg_days_between_orders
    from orderlines_agg as ola
    left join {{ ref('Orders')}} as o 
        on ola.customer_id = o.customer_id and ola.ba_site = o.ba_site
    {{dbt_utils.group_by(6)}}
) 

select 
    ba_site || '-' || customer_id as ba_site_customer_id,
    customer_id, 
    ba_site,
    days_since_last_order, 
    lifetime_orders, 
    lifetime_sales_amount, 
    lifetime_margin,
    avg_days_between_orders, 
    round(lifetime_sales_amount/lifetime_orders,2) as lifetime_sales_amount_aov
from customer_lifetime_stats
