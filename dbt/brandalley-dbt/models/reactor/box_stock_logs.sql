{{ config(materialized="table", tags=["job_daily"]) }}

SELECT cast(a.boxid as string) as boxid,
       cast(a.stockid as string) as reactor_sku,
       a.previous as previous_qty,
       timestamp_seconds(timestamp) as logged_at,
       a.scanned as scanned_qty,
       a.difference as difference_qty,
       b.name,
	   d.item_cost as unit_cost,
       a.difference*d.item_cost as difference_value
FROM {{ ref('stg__boxstockchecklog')}} a
LEFT JOIN {{ ref('stg__boxstockchecklogvia')}} b ON a.via=b.code
LEFT JOIN {{ ref('stg__stocklist')}} d ON a.stockid=d.id
LEFT JOIN {{ ref('stg__box')}} e ON a.boxid=e.id
WHERE date(timestamp_seconds(timestamp))>='2023-06-01'