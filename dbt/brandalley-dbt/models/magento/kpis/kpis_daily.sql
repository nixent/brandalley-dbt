{{ config(
    materialized='view'
)}}

with order_stats as (
    select
        date_trunc(datetime(o.created_at, "Europe/London"), day)                           as order_created_at_day,
        o.ba_site,
        count(distinct o.increment_id)                                                  as total_order_count,
        count(distinct if(o.orderno = 1, o.increment_id, null))                         as total_new_order_count,
        count(distinct if(ce.achica_user is not null and o.orderno = 1, o.customer_id, null))   as total_new_achica_order_count,
        count(distinct if(ce.is_new_ifg_user is not null and o.orderno = 1, o.customer_id, null))   as total_new_ifg_order_count,
        count(distinct if(o.orderno = 1, o.customer_id, null))                          as total_new_customer_count,
        count(distinct if(o.orderno > 1, o.customer_id, null))                          as total_existing_customer_count,
        sum(o.shipping_incl_tax)                                                        as shipping_amount
    from {{ ref('Orders')}} o
    left join {{ ref('customers_enriched') }} ce
        on o.customer_id = ce.customer_id
    group by 1,2
),

customer_stats as (
    select
        coalesce(date(crds.date), date(ce.achica_migration_date), date(datetime(ce.signed_up_at, "Europe/London"))) as customer_created_at_day,
        ce.ba_site,
        count(if(ce.achica_user is null, ce.customer_id, null))                                                     as total_new_members,
        count(if(ce.achica_user is not null, ce.customer_id, null))                                                 as total_new_achica_members,
        count(if(ce.is_new_ifg_user is not null, ce.customer_id, null))                                             as total_new_ifg_members
    from {{ ref('customers_enriched') }} ce
    left join {{ ref('customers_record_data_source') }} crds 
        on ce.customer_id = crds.cst_id
    group by 1,2
),

refund_stats as (
    select
        date_trunc(datetime(timestamp(sfc.created_at), "Europe/London"), day)       as order_created_at_day,
        sfc.ba_site,
        count(sfc.entity_id)                             as total_refund_count,
        count(sfci.entity_id)                            as total_item_refund_count,
        round(sum(sfci.base_row_total),2)                as total_refund_amount
    from {{ ref('sales_flat_creditmemo') }} sfc
    left join {{ ref('sales_flat_creditmemo_item') }} sfci
        on sfci.parent_id = sfc.entity_id and sfc.ba_site = sfci.ba_site
    group by 1,2
),

shipping_stats as (
    select
        date_trunc(datetime(timestamp(order_date), "Europe/London"), day)                                                                                    as order_created_at_day,
        ba_site,
        round(avg(date_diff(date((shipment_date)), date((order_date)), day)),1) as avg_time_to_ship_days
    from {{ ref('shipping') }}
    group by 1,2
),

order_line_stats as (
    select
        date_trunc(datetime(o.created_at, "Europe/London"), day)                                           as order_created_at_day,
        ol.ba_site,
        round(sum(ol.line_product_cost_exc_vat),2)                              as total_product_cost_exc_vat,
        round(sum(ol.qty_ordered),2)                                            as qty_ordered,
        round(sum(ol.total_local_currency_ex_tax_after_vouchers),2)                        as sales_amount,
        round(sum(if(s.has_shipped, ol.total_local_currency_ex_tax_after_vouchers, 0)),2)  as shipped_sales_amount,
        round(sum(ol.total_local_currency_after_vouchers),2)                               as gmv,
        round(sum(ol.line_discount_amount),2)                                   as total_discount_amount,
        round(sum(ol.margin),2)                                                 as margin
    from {{ ref('OrderLines') }} ol
    left join {{ ref('Orders') }} o
        on ol.order_number = o.increment_id and ol.ba_site = o.ba_site
    left join (
        select 
            max(true) as has_shipped, 
            order_id, 
            sku,
            ba_site
        from {{ ref('shipping') }}
        group by 2,3,4
     ) s
        on o.increment_id = s.order_id and ol.sku = s.sku and ol.ba_site = s.ba_site
    group by 1,2
),

conversion_stats as (
    select
        ga_session_at_date as ga_session_at_day,
        conversion_rate
    from {{ ref('ga_conversion_rate') }}
    where date_aggregation_type = 'day'
),

cs_stats as (
    select
        date_trunc(date, day)          as cs_tickets_day,
        sum(chat_ticket)               as chat_tickets,
        sum(email_ticket)              as email_tickets,
        sum(phone_ticket)              as phone_tickets
    from {{ref('tickets_daily') }}
    group by 1
)

select
    os.order_created_at_day || '-' || os.ba_site as ba_site_created_date,
    os.order_created_at_day,
    os.ba_site,
    os.total_order_count,
    os.total_new_order_count,
    os.total_new_achica_order_count,
    os.total_new_ifg_order_count,
    os.total_new_customer_count,
    os.total_existing_customer_count,
    cs2.total_new_members,
    cs2.total_new_achica_members,
    cs2.total_new_ifg_members,
    os.shipping_amount,
    rs.total_refund_count,
    rs.total_item_refund_count,
    rs.total_refund_amount,
    ss.avg_time_to_ship_days,
    ols.total_product_cost_exc_vat,
    ols.qty_ordered,
    ols.gmv,
    ols.sales_amount,
    ols.shipped_sales_amount,
    ols.total_discount_amount,
    ols.margin,
    round(ols.gmv/os.total_order_count,2)                as aov_gmv,
    round(ols.sales_amount/os.total_order_count,2)       as aov_sales,
    round(ols.qty_ordered/os.total_order_count,2)        as avg_items_per_order,
    round(100*rs.total_refund_amount/ols.sales_amount,2) as pct_sales_amount_refunded,
    round(100*safe_divide(css.phone_tickets + css.chat_tickets + css.email_tickets,os.total_order_count),2) as pct_cs_cpo,
    cs.conversion_rate
from order_stats os
left join refund_stats rs
    on os.order_created_at_day = rs.order_created_at_day and os.ba_site = rs.ba_site
left join shipping_stats ss
    on os.order_created_at_day = ss.order_created_at_day and os.ba_site = ss.ba_site
left join order_line_stats ols
    on os.order_created_at_day = ols.order_created_at_day and os.ba_site = ols.ba_site
left join conversion_stats cs
    on os.order_created_at_day = datetime(cs.ga_session_at_day) and os.ba_site = 'UK'
left join customer_stats cs2
    on os.order_created_at_day = cs2.customer_created_at_day and os.ba_site = cs2.ba_site
left join cs_stats css
    on os.order_created_at_day = css.cs_tickets_day and os.ba_site = 'UK'
