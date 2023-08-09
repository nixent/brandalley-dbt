{{ config(schema='marketing', materialized='incremental', tags=["job_daily"]) }}


with
    ga as (

        select
            gds.traffic_campaign,
            gds.date,
            gds.traffic_source,
            count(distinct transaction_id) as transaction_count,
            count(distinct visitor_id) as visitor_count,
            count(distinct unique_visit_id) as visit_count,
            --sum(case when oe.is_first_order = true then 1 else 0 end) as total_new_customers,
            count(distinct case when oe.is_first_order = true then oe.customer_id else null end) as total_new_customers,
            sum(ol.total_local_currency_after_vouchers) as gmv
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
        where gds.date >= '2022-06-01'
        {% if is_incremental() %} and date > (select max(date) from {{ this }}) {% endif %}
        group by gds.traffic_campaign, gds.date, gds.traffic_source

    ),

    ads as (

        select
            campaign_name,
            date,
            'google' as traffic_source,
            sum(clicks) as total_click,
            sum(impressions) as total_impressions,
            sum(spend) as total_spend
        from {{ ref("google_ads_campaign_stats") }} gacs
        where gacs.date >= '2022-06-01'
        {% if is_incremental() %} and date > (select max(date) from {{ this }}) {% endif %}
        group by campaign_name, date

        union all

        select
            campaign_name,
            date_day as date,
            'facebook' as traffic_source,
            sum(clicks) as total_click,
            sum(impressions) as total_impressions,
            sum(spend) as total_spend
        from {{ ref("facebook_ads_ad_report") }} faar
        where faar.date_day >= '2022-06-01'
        {% if is_incremental() %} and date_day > (select max(date) from {{ this }}) {% endif %}
        group by campaign_name, date_day

    )

select
    ifnull(ga.traffic_campaign,ads.campaign_name) as traffic_campaign,
    ifnull(ga.date, ads.date) as date,
    ifnull(ga.traffic_source, ads.traffic_source) as traffic_source,
    ga.transaction_count,
    ga.visitor_count,
    ga.visit_count,
    ga.total_new_customers,
    ga.gmv,
    ads.total_click,
    ads.total_impressions,
    ads.total_spend
from ga 
full outer join
    ads
    on ga.traffic_campaign = ads.campaign_name
    and ga.date = ads.date
    and ga.traffic_source = ads.traffic_source

