{{ config(materialized="incremental") }}


with
    ga as (

        select
            a.traffic_campaign,
            a.date,
            a.traffic_source,
            count(distinct transaction_id) as transaction_count,
            count(distinct visitor_id) as visitor_count,
            count(distinct visit_id) as visit_count,
            sum(
                case when c.is_first_order = true then 1 else 0 end
            ) as total_new_customers,
            sum(b.total_local_currency_after_vouchers) as gmv
        from {{ ref("ga_daily_stats") }} a
        left join
            {{ ref("OrderLines") }} b
            on a.transaction_id = b.order_number
            and a.product_sku = b.parent_sku
            and a.product_sku_offset = b.parent_sku_offset
            and b.ba_site = 'UK'
        left join
            {{ ref("orders_enriched") }} c
            on b.order_id = c.order_id
            and b.ba_site = c.ba_site
        where a.date >= '2022-01-01'
        group by a.traffic_campaign, a.date, a.traffic_source

    ),

    ads as (

        select
            campaign_name,
            date,
            'google' as traffic_source,
            sum(clicks) as total_click,
            sum(impressions) as total_impressions,
            sum(spend) as total_spend
        from {{ ref("google_ads_campaign_stats") }} a
        where a.date >= '2022-01-01'
        group by campaign_name, date

        union all

        select
            campaign_name,
            date_day as day,
            'facebook' as traffic_source,
            sum(clicks) as total_click,
            sum(impressions) as total_impressions,
            sum(spend) as total_spend
        from {{ ref("facebook_ads_ad_report") }} a
        where a.date_day >= '2022-01-01'
        group by campaign_name, date_day

    )

select
    a.traffic_campaign,
    a.date,
    ifnull(a.traffic_source, b.traffic_source) as traffic_source,
    a.transaction_count,
    a.visitor_count,
    a.visit_count,
    a.total_new_customers,
    a.gmv,
    b.total_click,
    b.total_impressions,
    b.total_spend
from ga a
full outer join
    ads b
    on a.traffic_campaign = b.campaign_name
    and a.date = b.date
    and a.traffic_source = b.traffic_source

{% if is_incremental() %} where date >= (select max(date) from {{ this }}) {% endif %}
