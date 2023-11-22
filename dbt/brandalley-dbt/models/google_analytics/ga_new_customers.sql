{{ config(
    materialized='incremental',
    unique_key=['logged_date','traffic_channel'],
    tags=["job_daily"]
)}}

with
    current_period as (
        select
            gds.date as logged_date,
            gds.traffic_channel,
            count(distinct case when oe.is_first_order = true then oe.customer_id else null end) as total_new_customers,
            count(distinct case when gds.is_new_user_registration = true then gds.visitor_id else null end) as total_new_registrations,
        from {{ ref("ga_daily_stats") }} gds
        left join
            {{ ref("OrderLines") }} ol
            on gds.transaction_id = ol.order_number
            and gds.product_sku = ol.parent_sku
            and gds.product_sku_offset = ol.parent_sku_offset
            and ol.ba_site = 'UK'
        left join
            {{ ref("orders_enriched") }} oe
            on ol.order_id = oe.order_id
            and ol.ba_site = oe.ba_site
        where gds.date >= '2022-11-01'
        {% if is_incremental() %} and gds.date > (select max(logged_date) from {{ this }}) {% endif %}
        group by 1, 2
    ),
    previous_period as (
        select
            a.traffic_channel,
            round(avg(total_new_customers),2) as last_12_months_avg_new_customers_total,
            round(avg(total_new_registrations),2) as last_12_months_avg_new_registrations_total
        {% if is_incremental() %}
        from {{this}} a {% else %} from current_period a {% endif %}
        where a.logged_date > date_sub(current_date, interval 1 year)
        group by 1
    )
select 
    a.*,
    b.last_12_months_avg_new_customers_total,
    b.last_12_months_avg_new_registrations_total
from current_period a 
left join previous_period b on a.traffic_channel=b.traffic_channel