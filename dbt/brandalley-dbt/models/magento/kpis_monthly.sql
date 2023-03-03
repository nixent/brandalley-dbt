{{ config(
    materialized='view'
)}}

with order_stats as (
    select
        date_trunc(created_at, month)                         as order_created_at_month,
        count(distinct increment_id)                          as total_order_count,
        count(distinct if(orderno=1, increment_id, null))     as total_new_order_count,
        sum(shipping_incl_tax)                                as shipping_amount
    from {{ ref('Orders')}}
    group by 1
),

refund_stats as (
    select
        date_trunc(timestamp(sfc.created_at), month)       as order_created_at_month,
        count(sfc.entity_id)                    as total_refund_count,
        count(sfci.entity_id)                   as total_item_refund_count,
        round(sum(sfci.base_row_total),2)       as total_refund_amount
    from {{ ref('sales_flat_creditmemo') }} sfc
    left join {{ ref('sales_flat_creditmemo_item') }} sfci
        on sfci.parent_id = sfc.entity_id
    group by 1
),

shipping_stats as (
    select
        date_trunc(timestamp(order_date), month) as order_created_at_month,
        round(avg(
            if(
                shipment_date != '0000-00-00 00:00:00', 
                1 + date_diff(date((shipment_date)), date((order_date)), day) 
                - 2 * date_diff(date((shipment_date)), date((order_date)), week) 
                - if(extract(dayofweek from date((order_date))) = 1, 1, 0) 
                - if(extract(dayofweek from date((order_date))) = 7, 1, 0)
                , null
                )
        ),1) as avg_time_to_ship_days
    from {{ ref('shipping') }}
    group by 1
),

order_line_stats as (
    select
        date_trunc(o.created_at, month)                                         as order_created_at_month,
        round(sum(ol.line_product_cost_inc_vat),2)                              as total_product_costs_inc_vat,
        round(sum(ol.line_product_cost_exc_vat),2)                              as line_product_cost_exc_vat,
        round(sum(ol.qty_ordered),2)                                            as qty_ordered,
        round(sum(ol.TOTAL_GBP_ex_tax_after_vouchers),2)                        as sales_amount,
        round(sum(if(s.has_shipped, ol.TOTAL_GBP_ex_tax_after_vouchers, 0)),2)  as shipped_sales_amount,
        round(sum(ol.TOTAL_GBP_after_vouchers),2)                               as gmv
    from {{ ref('OrderLines') }} ol
    left join {{ ref('Orders') }} o
        on ol.order_number = o.increment_id
    left join (
        select 
            max(true) as has_shipped, 
            order_id, 
            sku
        from {{ ref('shipping') }}
        group by 2,3
     ) s
        on o.increment_id = s.order_id and ol.sku = s.sku
    group by 1
),

conversion_stats as (
    select
        date_trunc(date, month)                           as ga_session_at_month,
        round(100*sum(transactions)/sum(unique_visits),2) as conversion_rate,
    from {{ ref('ga_daily_stats') }}
    group by 1
)

select
    os.order_created_at_month,
    os.total_order_count,
    os.total_new_order_count,
    os.shipping_amount,
    rs.total_refund_count,
    rs.total_item_refund_count,
    rs.total_refund_amount,
    ss.avg_time_to_ship_days,
    ols.total_product_costs_inc_vat,
    -- ols.line_product_cost_exc_vat,
    ols.qty_ordered,
    ols.gmv,
    ols.sales_amount,
    ols.shipped_sales_amount,
    ols.sales_amount - ols.total_product_costs_inc_vat   as margin,
    round(ols.gmv/os.total_order_count,2)                as aov_gmv,
    round(ols.sales_amount/os.total_order_count,2)       as aov_sales,
    round(ols.qty_ordered/os.total_order_count,2)        as avg_items_per_order,
    round(100*rs.total_refund_amount/ols.sales_amount,2) as pct_sales_amount_refunded,
    cs.conversion_rate
from order_stats os
left join refund_stats rs
    on os.order_created_at_month = rs.order_created_at_month
left join shipping_stats ss
    on os.order_created_at_month = ss.order_created_at_month
left join order_line_stats ols
    on os.order_created_at_month = ols.order_created_at_month
left join conversion_stats cs
    on os.order_created_at_month = timestamp(cs.ga_session_at_month)