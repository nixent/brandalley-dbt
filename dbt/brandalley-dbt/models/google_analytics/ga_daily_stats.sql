{{ config(
    materialized='incremental',
    unique_key='unique_key',
    cluster_by=['traffic_channel','product_brand'],
	partition_by = {
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

select
    {{dbt_utils.surrogate_key(['date', 'visitId', 'fullVisitorId', 'hits.hitNumber', 'product.productSKU', 'hits.transaction.transactionid', 'i'])}} as unique_key,
    parse_date("%Y%m%d", date)                                                                                as date,
    case when visitNumber = 1 then true else false end                                                        as is_new_user,
    channelGrouping                                                                                           as traffic_channel,
    trafficSource.medium                                                                                      as traffic_medium,
    trafficSource.campaign                                                                                    as traffic_campaign,
    trafficSource.source                                                                                      as traffic_source,
    device.browser                                                                                            as device_browser,
    device.operatingSystem                                                                                    as device_os,
    product.productSKU                                                                                        as product_sku,
    product.v2productName                                                                                     as product_name,
    product.productBrand                                                                                      as product_brand,
    product.productQuantity                                                                                   as product_quantity,
    hits.transaction.transactionid                                                                            as transaction_id,
    coalesce(product.productRevenue/1000000,0)                                                                as product_revenue,
    fullVisitorId || visitId                                                                                  as unique_visit_id
from {{ source('76149814', 'ga_sessions_*') }},
    unnest(hits) as hits
left join unnest(hits.product) as product with offset as i
where totals.visits = 1
    {% if is_incremental() %}
        and parse_date("%Y%m%d", date) >= current_date
    {% endif %}