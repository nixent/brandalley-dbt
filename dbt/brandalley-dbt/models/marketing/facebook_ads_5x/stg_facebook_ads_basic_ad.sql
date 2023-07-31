{{ config(schema='marketing', materialized='view', tags=["job_daily"]) }}

with basic_ad as (
    
    select 
        cast(ad_id as {{ dbt.type_bigint() }}) as ad_id,
        ad_name,
        adset_name as ad_set_name,
        date as date_day,
        cast(account_id as {{ dbt.type_bigint() }}) as account_id,
        impressions,
        coalesce(inline_link_clicks,0) as clicks,
        spend,
        reach,
        frequency
    from {{ source(
        'facebook_ads_5x',
        'basic_ad_daily'
    ) }} 
)

select * 
from basic_ad