{{ config(
    materialized='view'
)}}

with daily_metrics as (
    select
        date_trunc(datetime(ol.created_at, "Europe/London"), hour)                  as created_at_hour,
        ol.ba_site,
        ol.nego,
        ol.brand,
        ol.category_name,
        ol.department_type,
        ol.parent_sku,
        ol.product_type,
        ol.supplier_name,
        sum(ol.qty_invoiced)                                                    as qty_invoiced_metric,
        sum(ol.total_local_currency_after_vouchers)                             as total_revenue,
        sum(ol.line_product_cost_exc_vat)                                       as line_product_cost_exc_vat_metric,
        sum(round(ol.total_local_currency_ex_tax_after_vouchers,2))             as total_revenue_exc_tax,
        sum(ol.line_discount_amount)                                            as total_discount_amount,
        count(distinct ol.order_id)                                             as total_order_count,
        count(distinct if(oe.is_first_order = true, oe.order_id, null))               as total_new_order_count,
        count(distinct if(oe.is_first_order = false, oe.order_id, null))              as total_existing_order_count,
        sum(if(oe.is_first_order = true, ol.total_local_currency_after_vouchers, 0))  as nc_gmv,
        sum(if(oe.is_first_order = false, ol.total_local_currency_after_vouchers, 0)) as rc_gmv
    from {{ ref('OrderLines') }} ol
    left join {{ ref('orders_enriched') }} oe
        on ol.order_id = oe.order_id and ol.ba_site = oe.ba_site
    group by 1,2,3,4,5,6,7,8,9
)

select
    dm.created_at_hour,
    dm.ba_site,
    dm.nego,
    dm.brand,
    dm.category_name,
    dm.department_type,
    dm.parent_sku,
    dm.product_type,
    dm.supplier_name,
    dm.qty_invoiced_metric,
    dm.total_revenue,
    dm.line_product_cost_exc_vat_metric,
    dm.total_revenue_exc_tax,
    round(dm.total_revenue_exc_tax - dm.line_product_cost_exc_vat_metric,2)                                       as margin_value,
    dm.total_order_count,
    dm.rc_gmv,
    dm.nc_gmv,
    dm.total_discount_amount,
    dm.total_new_order_count,
    dm.total_existing_order_count,
    if(extract(hour from dm.created_at_hour) < 10, '0', '') || extract(hour from dm.created_at_hour) || ':00'     as hour,
from daily_metrics dm