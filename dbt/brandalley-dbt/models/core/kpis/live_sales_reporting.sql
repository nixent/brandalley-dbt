with
    every_15_mins as (
        select
            timestamp_sub(timestamp_add(
                cast(current_date as timestamp), interval x minute
            ), interval 7 day) as lower_boundary,
            timestamp_sub(datetime_add(
                timestamp_add(cast(current_date as timestamp), interval x minute),
                interval 899 second
            ), interval 7 day) as upper_boundary
        from
            unnest(
                generate_array(
                    0,
                    timestamp_diff(
                        datetime_add(cast(current_date as timestamp), interval 24 hour),
                        cast(current_date as timestamp),
                        minute
                    )
                )
            ) as x
        where
            extract(
                minute
                from timestamp_add(cast(current_date as timestamp), interval x minute)
            )
            in (0, 15, 30, 45)
            and date(timestamp_sub(timestamp_add(
                cast(current_date as timestamp), interval x minute
            ), interval 7 day))
            = date_sub(current_date, interval 7 day)
    )
select
    d.upper_boundary as timestamp,
    'Today' as period,
    count(distinct ol.order_number) as total_order_count,
    round(sum(ol.total_local_currency_after_vouchers), 2) as gmv,
from every_15_mins d
left join
    {{ ref("OrderLines") }} ol
    on ol.created_at between d.lower_boundary and d.upper_boundary
group by 1, 2
union all
select
    d.upper_boundary as timestamp,
    'Last Week' as period,
    count(distinct ol.order_number) as total_order_count,
    round(sum(ol.total_local_currency_after_vouchers), 2) as gmv,
from every_15_mins d
left join
    {{ ref("OrderLines") }} ol
    on timestamp_add(ol.created_at, interval 7 day) between d.lower_boundary and d.upper_boundary
group by 1, 2
union all
select
    d.upper_boundary as timestamp,
    'Same Date LY' as period,
    count(distinct ol.order_number) as total_order_count,
    round(sum(ol.total_local_currency_after_vouchers), 2) as gmv,
from every_15_mins d
left join
    {{ ref("OrderLines") }} ol
    on timestamp_add(ol.created_at, interval 364 day) between d.lower_boundary and d.upper_boundary
group by 1, 2
order by 1  