with order_stats as (
    select
        kd.order_created_at_day,
        kd.ba_site,
        kd.total_order_count,
        kd.total_new_customer_count,
        kd.total_new_achica_order_count,
        kd.total_new_cocosa_order_count,
        kd.total_new_ifg_order_count,
        kd.total_new_ba_order_count,
        kd.total_new_members,
        kd.total_new_achica_members,
        kd.total_new_cocosa_members,
        kd.total_new_ifg_members,
        kd.total_new_ba_members,
        kd.gmv,
        kd.sales_amount,
        kd.margin + if(kd.ba_site = 'FR', coalesce(ma.fr_amount,0) , coalesce(ma.uk_amount, 0)) as margin,
        kd.qty_ordered,
        kd.shipping_amount as shipping_gmv,
        kd.total_refund_count
    from {{ ref('kpis_daily')}} kd
    left join {{ ref('stg__margin_adjustments') }} ma
        on kd.order_created_at_day = ma.date and kd.ba_site = 'UK'
),

order_stats_yearly_average as (
    select
        kd.ba_site,
        round(avg(kd.total_order_count),2) as total_order_count,
        round(avg(kd.gmv),2) as gmv,
        round(avg(kd.total_refund_count),2) as total_refund_count
    from {{ ref('kpis_daily')}} kd
    left join {{ ref('stg__margin_adjustments') }} ma
        on kd.order_created_at_day = ma.date and kd.ba_site = 'UK'
    where kd.order_created_at_day > date_sub(current_date, interval 1 year)
    group by 1
),

customer_type_order_stats as (
    select
        date_trunc(if(ol.ba_site = 'FR',datetime(ol.created_at, "Europe/Paris"),datetime(ol.created_at, "Europe/London")), day) as order_created_at_day,
        ol.ba_site,
        count(distinct if(ce.customer_type = 'Achica', ol.order_number, null))                                                  as achica_orders,
        round(sum(if(ce.customer_type = 'Achica', ol.total_local_currency_after_vouchers, 0)),2)                                as achica_gmv,
        round(sum(if(ce.customer_type = 'Achica', ol.total_local_currency_ex_tax_after_vouchers, 0)),2)                         as achica_sales_amount,
        round(sum(if(ce.customer_type = 'Achica', ol.margin, 0)),2)                                                             as achica_margin,
        count(distinct if(ce.customer_type = 'IFG', ol.order_number, null))                                                     as ifg_orders,
        round(sum(if(ce.customer_type = 'IFG', ol.total_local_currency_after_vouchers, 0)),2)                                   as ifg_gmv,
        round(sum(if(ce.customer_type = 'IFG', ol.total_local_currency_ex_tax_after_vouchers, 0)),2)                            as ifg_sales_amount,
        round(sum(if(ce.customer_type = 'IFG', ol.margin, 0)),2)                                                                as ifg_margin,
        count(distinct if(ce.customer_type = 'Cocosa', ol.order_number, null))                                                  as cocosa_orders,
        round(sum(if(ce.customer_type = 'Cocosa', ol.total_local_currency_after_vouchers, 0)),2)                                as cocosa_gmv,
        round(sum(if(ce.customer_type = 'Cocosa', ol.total_local_currency_ex_tax_after_vouchers, 0)),2)                         as cocosa_sales_amount,
        round(sum(if(ce.customer_type = 'Cocosa', ol.margin, 0)),2)                                                             as cocosa_margin,
        count(distinct if(ce.customer_type = 'BA', ol.order_number, null))                                                      as ba_orders,
        round(sum(if(ce.customer_type = 'BA', ol.total_local_currency_ex_tax_after_vouchers, 0)),2)                             as ba_gmv,
        round(sum(if(ce.customer_type = 'BA', ol.total_local_currency_after_vouchers, 0)),2)                                    as ba_sales_amount,
        round(sum(if(ce.customer_type = 'BA', ol.margin, 0)),2)                                                                 as ba_margin
    from {{ ref('OrderLines') }} ol
    left join {{ ref('customers_enriched') }} ce
        on ol.customer_id = ce.customer_id
    group by 1,2
),

products_sales as (
    select
        string_agg(distinct name) as sales_launched,
        ba_site,
        date(sale_start)          as date
    from {{ ref('stg__catalog_category_entity_history') }}
    where type = 3
    group by 2,3
),

marketing_targets as (
    select
        target_date,
        ba_site,
        new_members_forecast                                                as new_members_target,
        new_customers_order_forecast                                        as new_customers_target,
        returning_customers_order_forecast + new_customers_order_forecast   as all_orders_target
    from {{ ref('marketing_targets') }}
),

yesterday_ga_stats as (
    select 
        parse_date("%Y%m%d", date)                  as ga_session_at_date,
        count(distinct fullVisitorId || visitId)    as ga_unique_visits
    from {{ source('76149814', 'ga_sessions_intraday_*') }}
    where _table_suffix = format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
        and totals.visits = 1
    group by 1
),

ga_stats as (
    select 
        gcr.ga_session_at_date,
        gcr.conversion_rate,
        gcr.ga_unique_visitors,
        coalesce(gcr.ga_unique_visits, ygs.ga_unique_visits) as ga_unique_visits
    from {{ ref('ga_conversion_rate') }} gcr
    left join yesterday_ga_stats ygs on gcr.ga_session_at_date = ygs.ga_session_at_date
    where gcr.date_aggregation_type = 'day'
),

ga_stats_yearly_average as (
    select 
        'UK' as ba_site,
        round(avg(gcr.conversion_rate),2) as conversion_rate,
        round(avg(gcr.ga_unique_visitors),0) as unique_visitors
    from {{ ref('ga_conversion_rate') }} gcr
    where gcr.date_aggregation_type = 'day' and gcr.ga_session_at_date > date_sub(current_date, interval 1 year)
)

select
    d.date_day,
    d.last_year_same_day,
    d.last_year,
    os.ba_site,
    gs.ga_unique_visits,
    gs.ga_unique_visitors,
    gs.conversion_rate,
    os.total_order_count,
    os.total_refund_count,
    os.total_new_customer_count,
    os.total_new_achica_order_count,
    os.total_new_cocosa_order_count,
    os.total_new_ifg_order_count,
    os.total_new_ba_order_count,
    os.total_new_members,
    os.total_new_achica_members,
    os.total_new_cocosa_members,
    os.total_new_ifg_members,
    os.total_new_ba_members,
    os.gmv,
    os.sales_amount,
    os.margin,
    os.qty_ordered,
    os.shipping_gmv,
    ctos.achica_orders,
    ctos.achica_gmv,
    ctos.achica_sales_amount,
    ctos.achica_margin,
    ctos.ifg_orders,
    ctos.ifg_gmv,
    ctos.ifg_sales_amount,
    ctos.ifg_margin,
    ctos.cocosa_orders,
    ctos.cocosa_gmv,
    ctos.cocosa_sales_amount,
    ctos.cocosa_margin,
    ctos.ba_orders,
    ctos.ba_gmv,
    ctos.ba_sales_amount,
    ctos.ba_margin,
    gs1.ga_unique_visits            as last_year_same_day_ga_unique_visits,
    gs1.ga_unique_visitors          as last_year_same_day_ga_unique_visitors,
    gs1.conversion_rate             as last_year_same_day_conversion_rate,
    gs2.ga_unique_visits            as last_year_ga_unique_visits,
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
    mt.new_members_target,
    mt.all_orders_target,
    mt.new_customers_target,
    dt.gmv_target,
    dt.sales_amount_target,
    dt.margin_target,
    dt.aov_target,
    dt.avg_units_target,
    dt.effective_avg_vat_rate,
    ps.sales_launched,
    ps_ly.sales_launched            as sales_launched_ly,
    osya.total_order_count          as last_12_months_avg_total_order_count,
    osya.total_refund_count         as last_12_months_avg_total_refund_count,
    osya.gmv                        as last_12_months_avg_gmv,
    gsya.conversion_rate            as last_12_months_avg_conversion_rate,
    gsya.unique_visitors            as last_12_months_avg_unique_visitors
from {{ ref('dates') }} d
left join order_stats os
    on os.order_created_at_day = d.date_day
left join order_stats os3
    on os3.order_created_at_day = d.last_year and os.ba_site = os3.ba_site
left join order_stats os4
    on os4.order_created_at_day = d.last_year_same_day and os.ba_site = os4.ba_site
left join ga_stats gs 
    on gs.ga_session_at_date = d.date_day and os.ba_site = 'UK'
left join ga_stats gs1
    on gs1.ga_session_at_date = d.last_year_same_day and os.ba_site = 'UK'
left join ga_stats gs2
    on gs2.ga_session_at_date = d.last_year and os.ba_site = 'UK'
left join marketing_targets mt 
    on d.date_day = mt.target_date and os.ba_site = mt.ba_site
left join {{ ref('daily_targets') }} dt
    on d.date_day = dt.date_day and os.ba_site = dt.ba_site
left join products_sales ps
    on d.date_day = ps.date and os.ba_site = ps.ba_site
left join products_sales ps_ly
    on d.last_year_same_day = ps_ly.date and os.ba_site = ps_ly.ba_site
left join customer_type_order_stats ctos 
    on d.date_day = ctos.order_created_at_day and os.ba_site = ctos.ba_site
left join order_stats_yearly_average osya
    on os.ba_site = osya.ba_site
left join ga_stats_yearly_average gsya 
    on os.ba_site = gsya.ba_site
where d.up_to_current_date_flag=True