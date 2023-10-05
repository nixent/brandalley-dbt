{{ config(
    materialized='table'
)}}

with daily_dates as (
    select
        date_day
    from unnest(generate_date_array('2020-12-01', current_date, interval 1 day)) as date_day
),

cal as (
  select 
  date_day, 
  cast(format_date('%Y', date_day) as int64) as year, 
  cast(format_date('%V', date_day) as int64) as week_num, 
  format_date('%A', date_day) as weekday_name 
  from daily_dates
)

select 
    c1.date_day,
    date_sub(c1.date_day, interval 1 year)     as last_year, 
    date_sub(c1.date_day, interval 1 month)    as last_month, 
    date_sub(c1.date_day, interval 1 week)     as last_week,
    c2.date_day                                as last_year_same_day,
    case when c1.date_day > date_sub(current_date, interval 1 year) then 1 else 0 end as last_12_months_flag 
 from cal c1
 inner join cal c2 
  on c1.year = c2.year+1 and c1.week_num = c2.week_num and c1.weekday_name = c2.weekday_name
