{{ config(
    materialized = 'incremental',
    unique_key = ['reservation_id', 'product_id'],
    cluster_by = ['reservation_id', 'product_id'],
    enabled=false
) }}

with streamkap_source as (
{{ streamkap_incremental_on_source_to_current(
    source_name = 'catalog_product_reservation_upload',
    id_field = config.get('unique_key')
) }}
)
,
table_cte as (
    SELECT
    {{dbt_utils.generate_surrogate_key(['reservation_id', 'product_id'])}} as composite_key,
    *
    from streamkap_source
)
select * from table_cte 
