{{ config(
  schema = 'facebook_ads_5x',
  materialized='table'
)}}

with report as (

    select *
    from {{ ref('stg_facebook_ads_basic_ad') }} 

), 

accounts as (

    select *
    from {{ ref('stg_facebook_ads_account_history') }} 
    where is_most_recent_record = true

),

campaigns as (

    select *
    from {{ ref('stg_facebook_ads_campaign_history') }} 
    where is_most_recent_record = true

),

ad_sets as (

    select *
    from {{ ref('stg_facebook_ads_ad_set_history') }} 
    where is_most_recent_record = true

),

ads as (

    select *
    from {{ ref('stg_facebook_ads_ad_history') }} 
    where is_most_recent_record = true

),

joined as (

    select 
        report.date_day,
        accounts.account_id,
        accounts.account_name,
        campaigns.campaign_id,
        campaigns.campaign_name,
        ad_sets.ad_set_id,
        ad_sets.ad_set_name,
        ads.ad_id,
        ads.ad_name,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend
    from report 
    left join accounts
        on report.account_id = accounts.account_id
    left join ads 
        on report.ad_id = ads.ad_id
    left join campaigns
        on ads.campaign_id = campaigns.campaign_id
    left join ad_sets
        on ads.ad_set_id = ad_sets.ad_set_id
    group by 1,2,3,4,5,6,7,8,9
)

select *
from joined