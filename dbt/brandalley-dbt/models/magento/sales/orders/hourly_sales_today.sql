{{ config(
    materialized='view'
)}}

with metrics_today_and_last_week as (
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
    where date(datetime(ol.created_at, "Europe/London")) in (current_date, current_date - 7)
    group by 1,2,3,4,5,6,7,8,9
),

customer_stats as (
    select
        date_trunc(coalesce(datetime(crds.date), datetime(ce.achica_migration_date), datetime(ce.cocosa_signup_at), datetime(ce.signed_up_at, "Europe/London")), hour) as customer_created_at_hour,
        ce.ba_site,
        count(ce.customer_id)                                                                                       as total_new_members,
        count(if(ce.achica_user is not null, ce.customer_id, null))                                                 as total_new_achica_members,
        count(if(ce.cocosa_user is not null, ce.customer_id, null))                                                 as total_new_cocosa_members,
        count(if(ce.is_new_ifg_user = true, ce.customer_id, null))                                                  as total_new_ifg_members
    from {{ ref('customers_enriched') }} ce
    left join {{ ref('customers_record_data_source') }} crds 
        on ce.customer_id = crds.cst_id
    group by 1,2
)

select
    coalesce(mt.created_at_hour, cs.customer_created_at_hour) as created_at_hour,
    coalesce(mt.ba_site, cs.ba_site) as ba_site,
    mt.nego,
    mt.brand,
    mt.category_name,
    mt.department_type,
    mt.parent_sku,
    mt.product_type,
    mt.supplier_name,
    mt.qty_invoiced_metric,
    mt.total_revenue,
    mt.line_product_cost_exc_vat_metric,
    mt.total_revenue_exc_tax,
    round(mt.total_revenue_exc_tax - mt.line_product_cost_exc_vat_metric,2)                                       as margin_value,
    mt.total_order_count,
    mt.rc_gmv,
    mt.nc_gmv,
    mt.total_discount_amount,
    mt.total_new_order_count,
    mt.total_existing_order_count,
    cs.total_new_members,
    if(extract(hour from mt.created_at_hour) < 10, '0', '') || extract(hour from mt.created_at_hour) || ':00'     as hour,
    null                                                                                                    as last_week_margin,
    null                                                                                                    as last_week_qty_invoiced,
    null                                                                                                    as last_week_revenue
from metrics_today_and_last_week mt
left outer join customer_stats cs
    on mt.created_at_hour = cs.customer_created_at_hour and mt.ba_site = cs.ba_site
where date(mt.created_at_hour) = current_date

union all

select
    mt.created_at_hour,
    mt.ba_site,
    mt.nego,
    mt.brand,
    mt.category_name,
    mt.department_type,
    mt.parent_sku,
    mt.product_type,
    mt.supplier_name,
    null                                                                                                    as qty_invoiced_metric,
    null                                                                                                    as total_revenue,
    null                                                                                                    as line_product_cost_exc_vat_metric,
    null                                                                                                    as total_revenue_exc_tax,
    null                                                                                                    as margin_value,
    null                                                                                                    as gmv_aov,
    null                                                                                                    as rc_aov,
    null                                                                                                    as nc_aov,
    null                                                                                                    as total_discount_amount,
    null                                                                                                    as total_new_order_count,
    null                                                                                                    as total_existing_order_count,
    null                                                                                                    as total_new_members,
    if(extract(hour from mt.created_at_hour) < 10, '0', '') || extract(hour from mt.created_at_hour) || ':00'     as hour,
    round(mt.total_revenue_exc_tax - mt.line_product_cost_exc_vat_metric,2)                                       as last_week_margin,
    mt.qty_invoiced_metric                                                                                     as last_week_qty_invoiced,
    mt.total_revenue_exc_tax                                                                                   as last_week_revenue
from metrics_today_and_last_week mt
where date(mt.created_at_hour) = current_date - 7

