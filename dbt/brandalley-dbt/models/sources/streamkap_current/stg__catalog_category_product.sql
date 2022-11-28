
{{ config(
    materialized = 'incremental',
    unique_key = ['category_id', 'product_id']
) }}

with streamkap_source as (
{{ streamkap_incremental_on_source_to_current(
    source_name = 'catalog_category_product',
    id_field = config.get('unique_key')
) }}
)
,
table_cte as (
    SELECT
    {{dbt_utils.surrogate_key(['category_id','product_id'])}} as composite_key,
    *
    from streamkap_source
)
select * from table_cte 
