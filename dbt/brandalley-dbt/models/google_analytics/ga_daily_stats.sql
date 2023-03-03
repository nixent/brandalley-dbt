{{ config(
    materialized='incremental',
    unique_key='date'
)}}

select
    parse_date("%Y%m%d", date)                                                                                                              as date,
    count(distinct hits.transaction.transactionid)                                                                                          as transactions,
    sum(hits.transaction.transactionrevenue)/1000000                                                                                        as revenue,
    count(distinct concat(cast(fullvisitorid as string), cast(visitstarttime as string)))                                                   as unique_visits,
    count(distinct hits.transaction.transactionid) / count(distinct concat(cast(fullvisitorid as string), cast(visitstarttime as string)))  as ecommerce_conversion_rate
from {{ source('76149814', 'ga_sessions_*') }},
    unnest(hits) as hits
where totals.visits = 1
    {% if is_incremental() %}
        and parse_date("%Y%m%d", date) > (select max(date) from {{this}})
    {% endif %}
group by date