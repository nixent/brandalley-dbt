{{ config(
    materialized='table', 
    tags=["job_daily"],
    cluster_by='date_day'
)}}

with order_line_daily as (
    select
        date(created_at) as date_day,
        ba_site,
        sku,
        parent_sku,
        brand,
        product_type,
        sum(qty_invoiced)                 as qty_invoiced,
        sum(warehouse_qty)                as warehouse_qty,
        sum(line_product_cost_exc_vat)    as total_product_cost_exc_vat,
        sum(line_flash_price_inc_vat)     as total_flash_price_inc_vat
    from {{ ref('OrderLines') }}
    where date(created_at) >= '2022-12-01'
    group by 1,2,3,4,5,6
),

order_line_recalc as (
    select
        d.date_day,
        p.ba_site,
        p.sku,
        p.parent_sku,
        p.brand,
        p.product_type,
        p.product_name,
        p.department_type,
        sum(ol2.qty_invoiced)                 as qty_invoiced_l4w,
        sum(ol2.warehouse_qty)                as warehouse_qty_l4w
    from (
        select ba_site, variant_sku as sku, sku as parent_sku, brand, product_type, name as product_name, department_type, min(date(dt_cr)) as min_date_day 
        from {{ ref('products') }} 
        group by 1,2,3,4,5,6,7
    ) p,
    {{ ref('dates') }} d
    left join order_line_daily ol2 
        on d.date_day >= ol2.date_day 
            and d.date_day - 28 <= ol2.date_day
            and p.ba_site = ol2.ba_site
            and p.sku = ol2.sku
    where d.up_to_current_date_flag = true and p.min_date_day <= d.date_day and d.date_day >= '2023-01-01'
    group by 1,2,3,4,5,6,7,8
)

select
    p2.date_day,
    p2.ba_site,
    p2.sku,
    p2.parent_sku,
    p2.product_name,
    p2.brand,
    p2.product_type,
    p2.department_type,
    p2.qty_invoiced_l4w,
    p2.warehouse_qty_l4w,
    min(ol3.date_day)                     as min_ytd_date,
    sum(ol3.qty_invoiced)                 as qty_invoiced_ytd,
    sum(ol3.warehouse_qty)                as warehouse_qty_ytd,
    sum(ol3.total_product_cost_exc_vat)   as total_product_cost_exc_vat,
    sum(ol3.total_flash_price_inc_vat)    as total_flash_price_inc_vat
from order_line_recalc p2
left join order_line_daily ol3 
    on p2.date_day >= ol3.date_day 
        and date_trunc(p2.date_day, year) <= ol3.date_day
        and p2.ba_site = ol3.ba_site
        and p2.sku = ol3.sku
group by 1,2,3,4,5,6,7,8,9,10


