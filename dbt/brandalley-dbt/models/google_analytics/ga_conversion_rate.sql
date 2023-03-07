{{ config(
    materialized='incremental',
    unique_key=['date_aggregation_type', 'ga_session_at_date']
)}}

select
    'month'                                                                                     as date_aggregation_type,
    date_trunc(date, month)                                                                     as ga_session_at_date,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date >= date_trunc(current_date, month)
{% endif %}
group by 1,2

union all

select
    'week'                                                                                      as date_aggregation_type,
    date_trunc(date, week(monday))                                                              as ga_session_at_date,
    round(100*safe_divide(count(distinct transaction_id),count(distinct unique_visit_id)),2)    as conversion_rate
from {{ ref('ga_daily_stats') }}
{% if is_incremental() %}
    where date >= date_trunc(current_date, week(monday))
{% endif %}
group by 1,2