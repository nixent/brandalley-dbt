{{config(
    materialized='incremental',
    unique_key='ba_site_order_id_sku'
)}}

select
    'UK-' || order_id || '-' || sku as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
    *
from {{ ref('stg_uk__warehouse_order_allocation_kettering') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

{# union all

select
    'FR-' || order_id || '-' || sku as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
    *
from {{ ref('stg_fr__warehouse_order_allocation_kettering') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %} #}