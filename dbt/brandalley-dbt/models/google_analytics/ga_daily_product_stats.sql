{{ config(
    materialized='incremental',
    unique_key='date',
    cluster_by='traffic_channel',
    enabled=false,
	partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

select
    parse_date("%Y%m%d", date)                                                                  as date,
    case when visitNumber = 1 then true else false end                                          as is_new_user,
    channelGrouping                                                                             as traffic_channel,
    trafficSource.medium                                                                        as traffic_medium,
    trafficSource.campaign                                                                      as traffic_campaign,
    trafficSource.source                                                                        as traffic_source,
    device.browser                                                                              as device_browser,
    device.operatingSystem                                                                      as device_os,
    product.productSKU                                                                          as product_sku,
    product.v2productName                                                                       as product_name,
    product.productBrand                                                                        as product_brand,
    sum(product.productQuantity)                                                                as product_quantity,
    count(distinct hits.transaction.transactionid)                                              as transactions,
    coalesce(sum(product.productRevenue)/1000000,0)                                             as gmv,
    count(distinct fullVisitorId || visitId)                                                    as unique_visits
from {{ source('76149814', 'ga_sessions_*') }},
    unnest(hits) as hits,
    unnest(hits.product) as product
where totals.visits = 1
    {% if is_incremental() %}
        and parse_date("%Y%m%d", date) > (select max(date) from {{this}})
    {% endif %}
group by
  1,2,3,4,5,6,7,8,9,10,11