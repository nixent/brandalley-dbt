{{ config(
  schema = 'marketing',
  materialized='table',
  tags=["job_daily"]
)}}

with campaign_label as (

    select campaign_id, 
           campaign_name 
    from {{ source(
        'google_ads',
        'p_ads_CampaignLabel_2357552990'
    ) }}  
    group by 1,2
),

account_label as (

    select customer_id,
           customer_descriptive_name as account_name
    from {{ source(
        'google_ads',
        'p_ads_Customer_2357552990'
    ) }}  
    group by 1,2
),

campaign_stats as (

    select a.campaign_id,
           b.campaign_name,
           a.customer_id,
           c.account_name,
           sum(metrics_clicks) as clicks,
           sum(metrics_impressions) as impressions,
           sum(metrics_cost_micros/1000000) as spend,
           sum(metrics_conversions) as conversions,
           sum(metrics_interactions) as interactions,
           segments_device as device,
           segments_date as date
    from {{ source(
        'google_ads',
        'p_ads_CampaignBasicStats_2357552990'
    ) }} a 
    left join campaign_label b on a.campaign_id=b.campaign_id
    left join account_label c on a.customer_id=c.customer_id
    group by 1,2,3,4,10,11
)

select * from campaign_stats