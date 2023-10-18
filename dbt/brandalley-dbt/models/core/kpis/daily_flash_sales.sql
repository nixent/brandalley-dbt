with

    daily_dates as (
        select date_day, week_num
        from {{ ref("dates") }}
    ),
    
    grouped_sales as (
        select ba_site, category_name, sale_start_at, sale_end_at
        from {{ ref("products_sales") }} ps
        where category_name <> 'Outlet'
        group by 1, 2, 3, 4
    ),

    flash_sales as (
        select
            d.date_day,
            d.week_num,
            gs.ba_site,
            count(distinct gs.category_name) as live_sales_count
        from daily_dates d
        left join
            grouped_sales gs
            on d.date_day >= date(gs.sale_start_at)
            and d.date_day <= date(gs.sale_end_at)
        group by 1, 2, 3
    ),

    flash_sales_start as (
        select
            d.date_day,
            week_num,
            site.ba_site,
            ifnull(count(distinct gs_start.category_name), 0) as sales_starting_at_date,
            ifnull(count(distinct gs_end.category_name), 0) as sales_ending_at_date,
        from daily_dates d
        cross join (select 'UK' as ba_site
                    union all
                    select 'FR' as ba_site) site
        left join
            grouped_sales gs_start
            on d.date_day = date(gs_start.sale_start_at)
            and site.ba_site = gs_start.ba_site
        left join
            grouped_sales gs_end
            on d.date_day = date(gs_end.sale_end_at)
            and site.ba_site = gs_end.ba_site
        group by 1, 2, 3

    )

select
    fs.date_day,
    fs.week_num,
    fs.ba_site,
    fs.live_sales_count,
    round(avg_l12.avg_last_12_months_live_sales, 2) as avg_last_12_months_live_sales,
    ifnull(fss.sales_starting_at_date, 0) as sales_starting_at_date,
    ifnull(fss.sales_ending_at_date, 0) as sales_ending_at_date,
    sum(ifnull(fss1.sales_starting_at_date, 0)) as same_week_future_sales_starting,
    sum(ifnull(fss1.sales_ending_at_date, 0)) as same_week_future_sales_ending
from flash_sales fs
left join
    flash_sales_start fss on fs.date_day = fss.date_day and fs.ba_site = fss.ba_site
left join
    flash_sales_start fss1
    on fss.week_num = fss1.week_num
    and fss.ba_site = fss1.ba_site
    and fss.date_day < fss1.date_day
left join 
    (select ba_site, avg(live_sales_count) as avg_last_12_months_live_sales from flash_sales where date_day between date_sub(current_date, interval 1 year) and current_date group by 1) avg_l12
    on fs.ba_site=avg_l12.ba_site
group by 1, 2, 3, 4, 5, 6, 7