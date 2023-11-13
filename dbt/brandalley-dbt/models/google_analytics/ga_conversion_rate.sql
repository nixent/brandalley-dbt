{{ config(
    materialized='incremental',
    unique_key=['date_aggregation_type', 'ga_session_at_date'],
    tags=["job_daily"]
)}}

with current_period as (
select
    'month'                                                                                     as date_aggregation_type,
    date_trunc(date, month)                                                                     as ga_session_at_date,
    traffic_channel,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct visitor_id)                                                                  as ga_unique_visitors,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, month) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'month')
{% endif %}
group by 1,2,3

union all

select
    'week'                                                                                      as date_aggregation_type,
    date_trunc(date, week(monday))                                                              as ga_session_at_date,
    traffic_channel,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct visitor_id)                                                                  as ga_unique_visitors,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, week(monday)) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'week')
{% endif %}
group by 1,2,3

union all

select
    'day'                                                                                       as date_aggregation_type,
    date_trunc(date, day)                                                                       as ga_session_at_date,
    traffic_channel,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct visitor_id)                                                                  as ga_unique_visitors,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, day) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'day')
{% endif %}
group by 1,2,3


union all

select
    'quarter'                                                                                   as date_aggregation_type,
    date_trunc(date, quarter)                                                                   as ga_session_at_date,
    traffic_channel,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct visitor_id)                                                                  as ga_unique_visitors,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, quarter) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'quarter')
{% endif %}
group by 1,2,3
),
previous_period as (
select
            a.traffic_channel,
            avg(conversion_rate) as last_12_months_avg_conversion_rate,
        {% if is_incremental() %}
        from {{this}} a {% else %} from current_period a {% endif %}
        where a.ga_session_at_date > date_sub(current_date, interval 1 year) and date_aggregation_type='day'
        group by 1
)
select a.*, b.last_12_months_avg_conversion_rate from current_period a 
left join previous_period b on a.traffic_channel=b.traffic_channel and a.date_aggregation_type='day'