{{ config(
  materialized='table'
)}}

with
    number_of_pages_per_sessions as (
        select
            cast(a.original_event_at as date) as date,
            a.session_id,
            count(distinct a.page_url) as number_of_pages
        from {{ ref('page_views')}} a
        group by 1,2
    ),
    sessions_with_one_page as (
        select 
            a.date, 
            count(distinct a.session_id) as number_of_sessions_with_one_page
        from number_of_pages_per_sessions a
        where number_of_pages = 1
        group by 1
    ),
    joined as (
        select
            cast(original_event_at as date) as date,
            count(distinct session_id) as no_of_sessions,
            b.number_of_sessions_with_one_page
        from {{ ref('page_views')}} a
        left join sessions_with_one_page b on cast(original_event_at as date) = b.date
        group by cast(original_event_at as date), b.number_of_sessions_with_one_page
    )
select
    *,
    safe_divide(number_of_sessions_with_one_page, no_of_sessions)*100 as bounce_rate
from joined

