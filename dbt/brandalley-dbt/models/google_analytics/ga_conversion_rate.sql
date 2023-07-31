{{ config(
    materialized='incremental',
    unique_key=['date_aggregation_type', 'ga_session_at_date'],
    tags=["job_daily"]
)}}

select
    'month'                                                                                     as date_aggregation_type,
    date_trunc(date, month)                                                                     as ga_session_at_date,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, month) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'month')
{% endif %}
group by 1,2

union all

select
    'week'                                                                                      as date_aggregation_type,
    date_trunc(date, week(monday))                                                              as ga_session_at_date,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, week(monday)) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'week')
{% endif %}
group by 1,2

union all

select
    'day'                                                                                       as date_aggregation_type,
    date_trunc(date, day)                                                                       as ga_session_at_date,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, day) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'day')
{% endif %}
group by 1,2


union all

select
    'quarter'                                                                                   as date_aggregation_type,
    date_trunc(date, quarter)                                                                   as ga_session_at_date,
    count(distinct unique_visit_id)                                                             as ga_unique_visits,
    count(distinct transaction_id)                                                              as ga_orders,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date_trunc(date, quarter) >= (select max(ga_session_at_date) from {{this}} where date_aggregation_type = 'quarter')
{% endif %}
group by 1,2