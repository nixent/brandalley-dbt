{{ config(
    materialized='table'
)}}

select
    publisher_id,
    publisher_name,
from {{ source('analytics', 'affiliate_publishers_gsheet') }}