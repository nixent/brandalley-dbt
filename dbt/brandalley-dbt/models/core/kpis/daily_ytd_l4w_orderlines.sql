with order_line_ytd as (
    select distinct
        d.date_day,
        ol.ba_site,
        ol.sku,
        ol.parent_sku,
        ol.brand,
        ol.name,
        ol.product_type,
        sum(ol.qty_invoiced)     as qty_invoiced,
        sum(ol.warehouse_qty)    as warehouse_qty,
        sum(ol.line_product_cost_exc_vat)    as total_product_cost_exc_vat,
        sum(ol.line_flash_price_inc_vat)     as total_flash_price_inc_vat
    from {{ ref('dates') }} d
,{{ ref('OrderLines') }} ol
where 
date_trunc(if(ol.ba_site = 'FR',datetime(ol.created_at, "Europe/Paris"),datetime(ol.created_at, "Europe/London")), day)>=
DATE_TRUNC(d.date_day, YEAR)
and d.date_day>=date_trunc(if(ol.ba_site = 'FR',datetime(ol.created_at, "Europe/Paris"),datetime(ol.created_at, "Europe/London")), day)
group by 1,2,3,4,5,6,7 
--order by d.date_day, sku
),

order_line_l4w as (
    select distinct
        d.date_day,
        ol.ba_site,
        ol.sku,
        ol.parent_sku,
        ol.brand,
        ol.name,
        ol.product_type,
        sum(ol.qty_invoiced)     as qty_invoiced,
        sum(ol.warehouse_qty)    as warehouse_qty
    from {{ ref('dates') }} d
,{{ ref('OrderLines') }} ol
where 
date_trunc(if(ol.ba_site = 'FR',datetime(ol.created_at, "Europe/Paris"),datetime(ol.created_at, "Europe/London")), day)>=
date_sub(d.date_day, interval 28 day)
and d.date_day>=date_trunc(if(ol.ba_site = 'FR',datetime(ol.created_at, "Europe/Paris"),datetime(ol.created_at, "Europe/London")), day)
--and ol.sku in ('17488148','17056467')
group by 1,2,3,4,5,6,7 
--order by d.date_day, sku
)

select
    COALESCE(ytd.date_day, l4w.date_day) as date_day,
    COALESCE(ytd.ba_site, l4w.ba_site) AS ba_site,
    COALESCE(ytd.sku, l4w.sku) AS sku,
    COALESCE(ytd.parent_sku, l4w.parent_sku) AS parent_sku,
    COALESCE(ytd.brand, l4w.brand) AS brand,
    COALESCE(ytd.name, l4w.name) AS name,
    COALESCE(ytd.product_type, l4w.product_type) AS product_type,
    ytd.qty_invoiced AS qty_invoiced_ytd,
    ytd.warehouse_qty AS warehouse_qty_ytd,
    l4w.qty_invoiced AS qty_invoiced_l4w,
    l4w.warehouse_qty AS warehouse_qty_l4w,
    ytd.total_product_cost_exc_vat,
    ytd.total_flash_price_inc_vat
from order_line_ytd ytd 
full outer join order_line_l4w l4w on ytd.date_day = l4w.date_day and ytd.sku=l4w.sku
-- and ytd.parent_sku=l4w.parent_sku and ytd.brand=l4w.brand and ytd.name=l4w.name and ytd.product_type=l4w.product_type
--where ytd.sku in ('17488148','17056467')
--order by ytd.date_day