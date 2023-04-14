{{ config(enabled=false)}}

with unioned as (
    select
    {{ dbt_utils.star(
        ref('stg_uk__eav_attribute_option_value'),
        quote_identifiers=false
    ) }}
    from {{ ref('stg_uk__eav_attribute_option_value') }}
)

select
    'UK-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'UK'                                                            as ba_site,
from {{ ref('stg_uk__eav_attribute_option_value') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'UK' )
{% endif %}

union all

select
    'FR-' || {{ config.get('unique_key')|replace('ba_site_', '') }} as {{ config.get('unique_key') }},
    'FR'                                                            as ba_site,
from {{ ref('stg_fr__eav_attribute_option_value') }}
{% if is_incremental() %}
    where bq_last_processed_at > (select max(bq_last_processed_at) from {{this}} where ba_site = 'FR' )
{% endif %}

{# columns:
      - name: ba_site_composite_key
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: ba_site
        description: "Brand Alley Site" #}
