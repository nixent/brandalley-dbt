with

    daily_dates as (
        select date_day, cast(format_date('%V', date_day) as int64) as week_num
        from
            unnest(
                generate_date_array(
                    '2020-12-01',
                    date_add(current_date, interval 1 month),
                    interval 1 day
                )
            ) as date_day
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
            on d.date_day >= cast(gs.sale_start_at as date)
            and d.date_day <= cast(gs.sale_end_at as date)
        group by 1, 2, 3
    ),

    flash_sales_start as (
        select
            d.date_day,
            week_num,
            ps.ba_site,
            ifnull(count(distinct gs.category_name), 0) as sales_starting_at_date
        from daily_dates d
        cross join (select distinct ba_site from {{ ref("products_sales") }}) ps
        left join
            grouped_sales gs
            on d.date_day = cast(gs.sale_start_at as date)
            and ps.ba_site = gs.ba_site
        group by 1, 2, 3

    )

select
    fs.date_day,
    fs.week_num,
    fs.ba_site,
    fs.live_sales_count,
    ifnull(fss.sales_starting_at_date, 0) as sales_starting_at_date,
    sum(ifnull(fss1.sales_starting_at_date, 0)) as same_week_future_sales_starting
from flash_sales fs
left join
    flash_sales_start fss on fs.date_day = fss.date_day and fs.ba_site = fss.ba_site
left join
    flash_sales_start fss1
    on fss.week_num = fss1.week_num
    and fss.ba_site = fss1.ba_site
    and fss.date_day < fss1.date_day
group by 1, 2, 3, 4, 5
