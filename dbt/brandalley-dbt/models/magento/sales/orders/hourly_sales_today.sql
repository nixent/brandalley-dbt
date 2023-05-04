{{ config(
    materialized='view'
)}}

with metrics_today_and_last_week as (
    select
        date_trunc(datetime(ol.created_at, "Europe/London"), hour)                  as created_at_hour,
        ba_site,
        nego,
        brand,
        category_name,
        department_type,
        parent_sku,
        product_type,
        supplier_name,
        sum(ol.qty_invoiced)                             as qty_invoiced_metric,
        sum(ol.total_local_currency_after_vouchers)                 as total_revenue,
        sum(ol.line_product_cost_exc_vat)                as line_product_cost_exc_vat_metric,
        sum(round(ol.total_local_currency_ex_tax_after_vouchers,2)) as total_revenue_exc_tax
    from {{ ref('OrderLines') }} ol
    where date(datetime(ol.created_at, "Europe/London")) in (current_date, current_date - 7)
    group by 1,2,3,4,5,6,7,8,9
)

select
    created_at_hour,
    ba_site,
    nego,
    brand,
    category_name,
    department_type,
    parent_sku,
    product_type,
    supplier_name,
    qty_invoiced_metric,
    total_revenue,
    line_product_cost_exc_vat_metric,
    total_revenue_exc_tax,
    round(total_revenue_exc_tax - line_product_cost_exc_vat_metric,2)                                       as margin_value,
    if(extract(hour from created_at_hour) < 10, '0', '') || extract(hour from created_at_hour) || ':00'     as hour,
    null                                                                                                    as last_week_margin,
    null                                                                                                    as last_week_qty_invoiced,
    null                                                                                                    as last_week_revenue
from metrics_today_and_last_week mt
where date(created_at_hour) = current_date

union all

select
    created_at_hour,
    ba_site,
    nego,
    brand,
    category_name,
    department_type,
    parent_sku,
    product_type,
    supplier_name,
    null                                                                                                    as qty_invoiced_metric,
    null                                                                                                    as total_revenue,
    null                                                                                                    as line_product_cost_exc_vat_metric,
    null                                                                                                    as total_revenue_exc_tax,
    null                                                                                                    as margin_value,
    if(extract(hour from created_at_hour) < 10, '0', '') || extract(hour from created_at_hour) || ':00'     as hour,
    round(total_revenue_exc_tax - line_product_cost_exc_vat_metric,2)                                       as last_week_margin,
    qty_invoiced_metric                                                                                     as last_week_qty_invoiced,
    total_revenue_exc_tax                                                                                   as last_week_revenue
from metrics_today_and_last_week mt
where date(created_at_hour) = current_date - 7

