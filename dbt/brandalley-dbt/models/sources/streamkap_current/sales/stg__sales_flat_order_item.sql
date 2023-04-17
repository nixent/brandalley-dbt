{{config(
    materialized='incremental',
    unique_key='ba_site_item_id',
	  cluster_by='order_id',
    partition_by = {
      "field": "bq_last_processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

--to do fix when streamkap fix

with unioned as (
    select
    {{ dbt_utils.star(
        ref('stg_uk__sales_flat_order_item'),
        quote_identifiers=false
    ) }}
    from {{ ref('stg_uk__sales_flat_order_item') }}
)

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
from {{ ref('stg_uk__sales_flat_order_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
from {{ ref('stg_fr__sales_flat_order_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}