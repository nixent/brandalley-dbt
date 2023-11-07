{{ config(materialized="table") }}


select
    b.id as box_id,
    a.aislenumber,
    r.racknumber,
    z.name as zone_,
    concat(
        'Zone: ', z.name, ', Aisle: ', a.aislenumber, ', Rack: ', r.racknumber
    ) as box_name,
    cast(b.laststockcheck as date) as last_checked,
    case
        when date_diff(current_date, date(b.laststockcheck), day) > 182
        then 'Over 6 Months'
        when date_diff(current_date, date(b.laststockcheck), day) > 152
        then '5-6 Months'
        when date_diff(current_date, date(b.laststockcheck), day) > 121
        then '4-5 Months'
        when date_diff(current_date, date(b.laststockcheck), day) > 90
        then '3-4 Months'
        when date_diff(current_date, date(b.laststockcheck), day) > 60
        then '2-3 Months'
        when date_diff(current_date, date(b.laststockcheck), day) > 30
        then '1-2 Months'
        else 'Less Than 1 Month'
    end as last_stock_check_month
from {{ ref("stg__box") }} b
left join {{ ref("stg__boxracking") }} r on r.id = b.boxrackingid
left join {{ ref("stg__adboxaisle") }} a on a.aisleid = r.aisleid
left join {{ ref("stg__adboxzone") }} z on z.zoneid = a.zoneid
where
    b.id in (
        select distinct boxid
        from {{ ref("stg__boxstockchecklog") }}
        where date(timestamp_seconds(timestamp)) >= '2023-06-01'
    )
group by 1, 2, 3, 4, 5, 6, 7
