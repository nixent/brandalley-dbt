{{config(
    materialized='incremental',
    unique_key='ba_site_id',
	cluster_by='ba_site_id',
)}}

-- to do when streamkap fix

with unioned as (
    select
    {{ dbt_utils.star(
        ref('stg_uk__stock_prism_grn_item'),
        quote_identifiers=false
    ) }}
    from {{ ref('stg_uk__stock_prism_grn_item') }}
)

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
from {{ ref('stg_uk__stock_prism_grn_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
from {{ ref('stg_fr__stock_prism_grn_item') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}