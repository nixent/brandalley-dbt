{{config(
    materialized='incremental',
    unique_key='ba_site_po_id',
	cluster_by='ba_site_po_id',
)}}

-- TO DO when streamkap fix

with unioned as (
    select
    {{ dbt_utils.star(
        ref('stg_uk__catalog_product_po'),
        quote_identifiers=false
    ) }}
    from {{ ref('stg_uk__catalog_product_po') }}
)

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
from {{ ref('stg_uk__catalog_product_po') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
from {{ ref('stg_fr__catalog_product_po') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}