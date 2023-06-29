/* Dont think this logic is right, bounce rate of around 10% seems a bit low */

with pages_per_visit as (

select date,
       visit_id,
       count(distinct page_path) as pages_count, 
from {{ ref('ga_daily_stats') }}
group by 1,2

),

one_page_visits as (

select date,
       count(distinct visit_id) as visits_with_one_page
from pages_per_visit 
where pages_count=1
group by 1

),

joined as (

select a.date,
       count(distinct a.visit_id) as visits_count,
       b.visits_with_one_page
from {{ ref('ga_daily_stats') }} a
left join one_page_visits b on a.date=b.date
group by 1,3

)

select *, 
       round(100*safe_divide(visits_with_one_page,visits_count),2) as bounce_rate
from joined

