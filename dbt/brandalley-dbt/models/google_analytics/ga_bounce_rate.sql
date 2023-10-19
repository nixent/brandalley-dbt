{{ config(
    materialized='incremental',
    tags=["job_daily"]
)}}

with
    number_of_pages_per_sessions as (
        select
            cast(a.visit_start_at as date) as date,
            a.unique_visit_id,
            count(distinct a.page_path) as number_of_pages
        from {{ ref("ga_daily_stats") }} a
        where cast(a.visit_start_at as date) < current_date --so we only load full previous day at a time
        {% if is_incremental() %}
        and cast(a.visit_start_at as date) >= (select max(logged_date) from {{this}})
        {% endif %}
        group by 1, 2
    ),
    sessions_with_one_page as (
        select a.date, count(distinct a.unique_visit_id) as number_of_sessions_with_one_page
        from number_of_pages_per_sessions a
        where number_of_pages = 1
        group by 1
    ),
    bounce_rate as (
        select
            a.date,
            count(distinct a.unique_visit_id) as sessions_total,
            sum(a.number_of_pages) as page_views_total,
            b.number_of_sessions_with_one_page,
            safe_divide(b.number_of_sessions_with_one_page, count(distinct a.unique_visit_id))*100 as bounce_rate
        from number_of_pages_per_sessions a
        left join sessions_with_one_page b on a.date = b.date
        group by 1, 4
    ),
    average_last_12_months as ( --these are average of last 12 months to current date
        select
            avg(sessions_total) as last_12_months_avg_sessions_total,
            avg(page_views_total) as last_12_months_avg_page_views_total,
            avg(bounce_rate) as last_12_months_avg_bounce_rate
        from bounce_rate a
        inner join
            {{ ref("dates") }} b on a.date = b.date_day and b.last_12_months_flag = True
    )
select
    date as logged_date,
    sessions_total,
    page_views_total,
    number_of_sessions_with_one_page,
    bounce_rate,
    round(last_12_months_avg_sessions_total, 2) as last_12_months_avg_sessions_total,
    round(last_12_months_avg_page_views_total, 2) as last_12_months_avg_page_views_total,
    round(last_12_months_avg_bounce_rate, 2) as last_12_months_avg_bounce_rate
from bounce_rate a
cross join average_last_12_months b
