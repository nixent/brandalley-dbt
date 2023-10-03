{{ config(materialized="table", tags=["job_daily"]) }}

with goods_in_join as (
   select
        cast(current_date as date) as logged_date,
        p.variant_sku as sku,
        p.ba_site,
        wsrb.qty_remaining_kettering as stock_qty,
        p.cost as unit_cost,
        sum(rgi.qty_arrived) as qty_arrived, ---need to sum this here as causing issues in the sum() over when a sku has multiple arrivals on same day
        rgi.date_arrived
    from {{ ref("products") }} p
    left join {{ ref("stg__warehouse_stock_running_balance") }} wsrb
            on wsrb.sku = p.variant_sku
            and wsrb.ba_site = p.ba_site
    inner join {{ ref('stg__cataloginventory_stock_item') }} stock 
            on stock.product_id = p.variant_product_id
            and p.ba_site = stock.ba_site
    left join {{ ref("stg__reactor_goods_in") }} rgi
            on p.variant_sku = rgi.sku
            and cast(current_date as date) >= rgi.date_arrived  -- might need to think about this when france warehouse closes? join to reactor on sku?
    where stock.qty > 0 
    group by 1,2,3,4,5,7
),
running_qty as (
    select
        a.logged_date,
        a.sku,
        a.ba_site,
        a.stock_qty,
        a.unit_cost,
        a.qty_arrived,
        sum(a.qty_arrived) over (partition by a.sku, a.ba_site order by a.date_arrived desc) as running_quantity,
        ifnull(a.date_arrived, b.delivery_date) as date_arrived,
        ifnull(date_diff(a.logged_date, a.date_arrived, day),date_diff(a.logged_date, b.delivery_date, day)) as days_old
    from goods_in_join a
    left join (select sku, delivery_date, ba_site
               from {{ ref("stg__stock_prism_grn_item") }}
               qualify row_number() over (partition by sku, ba_site order by delivery_date desc)= 1) b
            on a.sku = b.sku
            and a.ba_site = b.ba_site
),
skus_seperated as (
    select
        logged_date,
        sku,
        ba_site,
        stock_qty,
        case when running_quantity <= stock_qty then qty_arrived
             when running_quantity is null then stock_qty
             else stock_qty - (running_quantity - qty_arrived) end as qty_split,
        date_arrived,
        days_old as days_old,
        unit_cost
    from running_qty a
    where case when running_quantity <= stock_qty then qty_arrived
               when running_quantity is null then stock_qty
               else stock_qty - (running_quantity - qty_arrived) end > 0
),
missing_deliveries as (
select *, sum(qty_split) over (partition by sku, ba_site) as summed_qty_split
from skus_seperated --fix skus which have some deliveries but not enough to cover the qty in stock. Find the difference and append it in the union with the bucket 'no deliveries'
)
select
    a.logged_date,
    a.sku,
    a.ba_site,
    a.qty_split,
    a.date_arrived,
    a.days_old,
    a.unit_cost,
    case when a.days_old > 365 then 'Over 12 Months'
         when a.days_old > 274 and a.days_old <= 365 then '10-12 Months'
         when a.days_old > 180 and a.days_old <= 274 then '7-9 Months'
         when a.days_old > 90 and a.days_old <= 180 then '4-6 Months'
         when a.days_old is null then 'No Deliveries'
         else '0-3 Months' end as age_bucket,
    round((sum((qty_split * days_old)) over (partition by sku, ba_site)) / (sum(qty_split) over (partition by sku, ba_site)),2) as sku_avg_weighted_age,
    case when a.days_old > 365 then 5
         when a.days_old > 274 and a.days_old <= 365 then 4
         when a.days_old > 180 and a.days_old <= 274 then 3
         when a.days_old > 90 and a.days_old <= 180 then 2
         when a.days_old is null then 6
         else 1 end as age_bucket_sort_order
from skus_seperated a
union all
select 
    a.logged_date,
    a.sku,
    a.ba_site,
    a.stock_qty - summed_qty_split as qty_split,
    cast(null as date) as date_arrived,
    null as days_old,
    a.unit_cost,
    'No Deliveries'
          as age_bucket,
    null as sku_avg_weighted_age,
    6 as age_bucket_sort_order
from missing_deliveries a
where a.stock_qty>summed_qty_split
group by 1,2,3,4,5,6,7,8,9