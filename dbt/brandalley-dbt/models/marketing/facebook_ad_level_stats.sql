{{ config(schema='marketing', materialized='incremental', tags=["job_daily"]) }}


with
    ads as (

        select
            campaign_name,
            ad_name,
            date_day as date,
            'facebook' as traffic_source,
            sum(clicks) as total_click,
            sum(impressions) as total_impressions,
            sum(spend) as total_spend,
            sum(reach) as reach
        from {{ ref("facebook_ads_ad_report") }} faar
        where faar.date_day >= '2022-06-01' and faar.date_day<current_date
        {% if is_incremental() %} and date_day > (select max(date) from {{ this }}) {% endif %}
        group by 1,2,3

    ),

    ga as (

        select
            gds.traffic_campaign,
            gds.traffic_ad_content,
            gds.date,
            gds.traffic_source,
            count(distinct transaction_id) as transaction_count,
            count(distinct visitor_id) as visitor_count,
            count(distinct unique_visit_id) as visit_count,
            count(distinct case when oe.is_first_order = true then oe.customer_id else null end) as total_new_customers,
            count(distinct case when gds.is_new_user_registration = true then gds.visitor_id else null end) as total_new_registrations,
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
        where gds.date >= '2022-06-01' and gds.traffic_source='facebook'
        {% if is_incremental() %} and date > (select max(date) from {{ this }}) {% endif %}
        group by 1,2,3,4

    )

select
    ifnull(ads.campaign_name, ga.traffic_campaign) as traffic_campaign,
    ifnull(ads.ad_name, ga.traffic_ad_content) as ad_name,
    ifnull(ads.date, ga.date) as date,
    ifnull(ads.traffic_source, ga.traffic_source) as traffic_source,
    ga.transaction_count,
    ga.visitor_count,
    ga.visit_count,
    ga.total_new_customers,
    ga.total_new_registrations,
    ga.gmv,
    ads.total_click,
    ads.total_impressions,
    ads.total_spend,
    ads.reach
from ads
full outer join
    ga
    on ads.campaign_name = ga.traffic_campaign
    and ads.date = ga.date
    and ads.traffic_source = ga.traffic_source
    and ads.ad_name = ga.traffic_ad_content

