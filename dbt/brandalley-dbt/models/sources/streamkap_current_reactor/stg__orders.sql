{{config(
    materialized='incremental',
    unique_key='orderid',
	cluster_by='orderid',
)}}

select
    orderid,
    suborderid,
    customerid,
    stockid,
    productid,
    quantity,
    dispatched,
    TIMESTAMP_MILLIS(leftwarehouse_timestamp) as leftwarehouse_timestamp,
    price,
    price_adjust,
    TIMESTAMP_MILLIS(ordered_timestamp) as ordered_timestamp,
    TIMESTAMP_MILLIS(completed_timestamp) as completed_timestamp,
    dispatch_warehouseid,
    ext_order_id,
    vat,
    exchange_rate,
    currency,
    eur_rate,
    deliverytype,
    updated_at,
    date_add('1970-01-01', interval ship_by day) as ship_by,
    priority,
    _streamkap_source_ts_ms,
    _streamkap_ts_ms,
    cast(__deleted as boolean) as __deleted,
    current_timestamp                  as bq_last_processed_at
from {{ source('streamkap_reactor', 'orders') }}
where 1=1
{% if is_incremental() %}
    and _streamkap_ts_ms > (select max(_streamkap_ts_ms) from {{this}})
{% endif %}
qualify ROW_NUMBER() over (PARTITION BY orderid ORDER BY _streamkap_source_ts_ms desc) = 1