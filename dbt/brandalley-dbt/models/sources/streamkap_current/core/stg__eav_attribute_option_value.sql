{{ config(
    materialized = 'incremental',
    unique_key = ['option_id', 'store_id'],
    cluster_by = ['option_id', 'store_id'],
) }}

with streamkap_source as (
{{ streamkap_incremental_on_source_to_current(
    source_name = 'eav_attribute_option_value',
    id_field = config.get('unique_key'),
    order_offset_field = 'value_id'
) }}
)
,
table_cte as (
    SELECT
    {{dbt_utils.generate_surrogate_key(['option_id','store_id'])}} as composite_key,
    *
    from streamkap_source
)
select * from table_cte 
