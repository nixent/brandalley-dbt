{{ config(
    materialized='view'
)}}

with metrics_today_and_last_week as (
    select
        date_trunc(ol.created_at, hour)                  as created_at_hour,
        sum(ol.qty_invoiced)                             as qty_invoiced_metric,
        sum(ol.TOTAL_GBP_after_vouchers)                 as total_revenue,
        sum(ol.line_product_cost_exc_vat)                as line_product_cost_exc_vat_metric,
        sum(round(ol.TOTAL_GBP_ex_tax_after_vouchers,2)) as total_revenue_exc_tax
    from {{ ref('OrderLines') }} ol
    where date(date_trunc(ol.created_at, day)) in (current_date, current_date - 7)
    group by 1
),

margin_calcs as (
    select
        *,
        round(((total_revenue_exc_tax - line_product_cost_exc_vat_metric)/nullif(total_revenue_exc_tax, 0))*100, 2) || '%'                             as margin_percent,
        round(total_revenue_exc_tax - line_product_cost_exc_vat_metric,2)                                                                              as margin_value,
        if(extract(hour from created_at_hour) < 10, '0', '') || extract(hour from created_at_hour) || ':00'                                            as hour,
        lag(total_revenue_exc_tax - line_product_cost_exc_vat_metric ) over (partition by extract(hour from created_at_hour) order by created_at_hour) as last_week_margin,
        round((
            (total_revenue_exc_tax - line_product_cost_exc_vat_metric 
                - lag(total_revenue_exc_tax - line_product_cost_exc_vat_metric) over (partition by extract(hour from created_at_hour) order by created_at_hour)))
            / lag(total_revenue_exc_tax - line_product_cost_exc_vat_metric) over (partition by extract(hour from created_at_hour) order by created_at_hour
        )*100,2)                                                                                                                                              as wow_margin_change_pct,
        lag(qty_invoiced_metric) over (partition by extract(hour from created_at_hour) order by created_at_hour)                                              as last_week_qty_invoiced,
        round((
            (qty_invoiced_metric 
                - lag(qty_invoiced_metric) over (partition by extract(hour from created_at_hour) order by created_at_hour)))
            / lag(qty_invoiced_metric) over (partition by extract(hour from created_at_hour) order by created_at_hour
        )*100,2)                                                                                                                                              as wow_qty_invoiced_change_pct,
        lag(total_revenue_exc_tax) over (partition by extract(hour from created_at_hour) order by created_at_hour)                                            as last_week_revenue,
        round((
            (total_revenue_exc_tax 
                - lag(total_revenue_exc_tax) over (partition by extract(hour from created_at_hour) order by created_at_hour)))
            / lag(total_revenue_exc_tax) over (partition by extract(hour from created_at_hour) order by created_at_hour
        )*100,2)                                                                                                                                              as wow_revenue_change_pct
    from metrics_today_and_last_week mt
)

select 
    *
from margin_calcs
where date(created_at_hour) = current_date