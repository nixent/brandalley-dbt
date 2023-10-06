{{ config(materialized="table") }}

with
    number_of_pages_per_sessions as (
        select
            cast(a.original_event_at as date) as date,
            a.session_id,
            count(distinct a.page_url) as number_of_pages
        from {{ ref("page_views") }} a
        group by 1, 2
    ),
    sessions_with_one_page as (
        select a.date, count(distinct a.session_id) as number_of_sessions_with_one_page
        from number_of_pages_per_sessions a
        where number_of_pages = 1
        group by 1
    ),
    bounce_rate as (
        select
            cast(original_event_at as date) as date,
            count(distinct session_id) as sessions_total,
            count(distinct concat(session_id, page_url)) as page_views_total,
            b.number_of_sessions_with_one_page,
            safe_divide(number_of_sessions_with_one_page, count(distinct session_id)) as bounce_rate
        from {{ ref("page_views") }} a
        left join sessions_with_one_page b on cast(original_event_at as date) = b.date
        group by 1, 4
    ),
    average_last_12_months as (
        select
            avg(sessions_total) as last_12_months_avg_sessions_total,
            avg(page_views_total) as last_12_months_avg_page_views_total,
            avg(bounce_rate) as last_12_months_avg_bounce_rate
        from bounce_rate a
        inner join
            {{ ref("dates") }} b on a.date = b.date_day and b.last_12_months_flag = 1
    )
select
    date as logged_date,
    sessions_total,
    page_views_total,
    bounce_rate,
    round(last_12_months_avg_sessions_total, 2) as last_12_months_avg_sessions_total,
    round(last_12_months_avg_page_views_total, 2) as last_12_months_avg_page_views_total,
    round(last_12_months_avg_bounce_rate, 2) as last_12_months_avg_bounce_rate
from bounce_rate a
cross join average_last_12_months b
