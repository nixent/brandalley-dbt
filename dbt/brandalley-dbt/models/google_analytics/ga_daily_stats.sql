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

{% if execute and is_incremental() %}
  {% set sql %}
    select replace(cast(max(date) as string), '-', '') as max_date from {{this}}
  {% endset %}
  {% set result = run_query(sql) %}
  {% set max_date = result.columns['max_date'][0]  %}
{% endif %}

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
    -- As we receive parent_sku as the sku - keep the offset to be able to join to OrderLines on a 1 to 1
    i                                                                                                         as product_sku_offset,
    product.v2productName                                                                                     as product_name,
    product.productBrand                                                                                      as product_brand,
    product.productQuantity                                                                                   as product_quantity,
    hits.transaction.transactionid                                                                            as transaction_id,
    coalesce(product.productRevenue/1000000,0)                                                                as product_revenue,
    fullVisitorId                                                                                             as visitor_id,
    visitId                                                                                                   as visit_id,
    fullVisitorId || visitId                                                                                  as unique_visit_id --,
    {# hits.experiment.experimentId                                                                              as experiment_id, 
    hits.experiment.experimentVariant                                                                         as experiment_variant #}
from `bigquery-347014.76149814.ga_sessions_20230326`,
{# from {{ source('76149814', 'ga_sessions_*') }}, #}
    unnest(hits) as hits
left join unnest(hits.product) as product with offset as i
where totals.visits = 1
    {% if is_incremental() %}
        and _table_suffix between '{{max_date}}' and format_date('%Y%m%d', date_sub(current_date(), interval 1 day))
    {% endif %}