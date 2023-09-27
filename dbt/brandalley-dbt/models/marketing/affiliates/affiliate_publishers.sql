{{ config(
    materialized='table'
)}}

select
    cast(publisher_id as string) as publisher_id,
    publisher_name,
from {{ source('analytics', 'affiliate_publishers_gsheet') }}
group by 1,2