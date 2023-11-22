{{ config(
    materialized='incremental',
    unique_key=['logged_date','traffic_channel'],
    tags=["job_daily"]
)}}

with
    number_of_pages_per_sessions as (
        select
            cast(a.visit_start_at as date) as logged_date,
            a.traffic_channel,
            a.unique_visit_id,
            a.visitor_id,
            count(distinct a.page_path) as number_of_pages
        from {{ ref("ga_daily_stats") }} a
        where cast(a.visit_start_at as date) < current_date and cast(a.visit_start_at as date) > date_sub(current_date, interval 1 month) --so we only load full previous day at a time
        {% if is_incremental() %}
        and cast(a.visit_start_at as date) >= (select max(logged_date) from {{this}})
        {% endif %}
        group by 1, 2, 3, 4
    ),
    bounce_rate as (
        select
            a.logged_date,
            a.traffic_channel,
            count(distinct a.unique_visit_id) as sessions_total,
            count(distinct a.visitor_id) as unique_visitors_total,
            sum(a.number_of_pages) as page_views_total,
            count(distinct (if (a.number_of_pages = 1, a.unique_visit_id, null))) as number_of_sessions_with_one_page,
            safe_divide(count(distinct (if (a.number_of_pages = 1, a.unique_visit_id, null))), count(distinct a.unique_visit_id))*100 as bounce_rate
        from number_of_pages_per_sessions a
        group by 1, 2
    ),
    average_last_12_months as ( --these are average of last 12 months to current date
        select
            a.traffic_channel,
            avg(sessions_total) as last_12_months_avg_sessions_total,
            avg(unique_visitors_total) as last_12_months_avg_unique_visitors_total,
            avg(page_views_total) as last_12_months_avg_page_views_total,
            avg(bounce_rate) as last_12_months_avg_bounce_rate
        {% if is_incremental() %}
        from {{this}} a {% else %} from bounce_rate a {% endif %}
        where a.logged_date > date_sub(current_date, interval 1 year)
        group by 1
    )
select
    logged_date,
    a.traffic_channel,
    sessions_total,
    unique_visitors_total,
    page_views_total,
    number_of_sessions_with_one_page,
    bounce_rate,
    round(last_12_months_avg_sessions_total, 2) as last_12_months_avg_sessions_total,
    round(last_12_months_avg_unique_visitors_total, 2) as last_12_months_avg_sessions_total,
    round(last_12_months_avg_page_views_total, 2) as last_12_months_avg_page_views_total,
    round(last_12_months_avg_bounce_rate, 2) as last_12_months_avg_bounce_rate
from bounce_rate a
left join average_last_12_months b on a.traffic_channel=b.traffic_channel
order by logged_date desc