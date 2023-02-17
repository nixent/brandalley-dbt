{{ config(
    materialized='view'
)}}

with metrics_today as (
    select
        date_trunc(ol.created_at, hour)         as created_at_hour,
        sum(ol.qty_invoiced)                    as qty_invoiced_metric,
        sum(ol.TOTAL_GBP_after_vouchers)        as total_revenue,
        sum(ol.line_product_cost_exc_vat)       as line_product_cost_exc_vat_metric,
        sum(ol.TOTAL_GBP_ex_tax_after_vouchers) as total_revenue_exc_tax
    from {{ ref('OrderLines') }} ol
    where date(date_trunc(ol.created_at, day)) = current_date
    group by 1
),

metrics_last_week as (
    select
        date_trunc(ol.created_at, hour)         as created_at_hour,
        sum(ol.qty_invoiced)                    as qty_invoiced_metric,
        sum(ol.TOTAL_GBP_after_vouchers)        as total_revenue,
        sum(ol.line_product_cost_exc_vat)       as line_product_cost_exc_vat_metric,
        sum(ol.TOTAL_GBP_ex_tax_after_vouchers) as total_revenue_exc_tax
    from {{ ref('OrderLines') }} ol
    where date(date_trunc(ol.created_at, day)) = current_date - 7
    group by 1
)
select
  mt.*,
  round(((mt.total_revenue_exc_tax - mt.line_product_cost_exc_vat_metric)/nullif(mt.total_revenue_exc_tax, 0))*100, 2) || '%' as margin_percent,
  '£' || format("%'.0f", mt.total_revenue_exc_tax - mt.line_product_cost_exc_vat_metric)                                      as margin_value,
  if(extract(hour from mt.created_at_hour) < 10, '0', '') || extract(hour from mt.created_at_hour) || ':00'                   as hour,
  mlw.total_revenue_exc_tax as total_revenue_exc_tax_last_week,
  mlw.qty_invoiced_metric as qty_invoiced_metric_last_week,
  round(((mlw.total_revenue_exc_tax - mlw.line_product_cost_exc_vat_metric)/nullif(mlw.total_revenue_exc_tax, 0))*100, 2) || '%' as margin_percent_last_week,
  '£' || format("%'.0f", mlw.total_revenue_exc_tax - mlw.line_product_cost_exc_vat_metric)                                      as margin_value_last_week
from metrics_today mt
left join metrics_last_week mlw
    on extract(hour from mt.created_at_hour) = extract(hour from mlw.created_at_hour)