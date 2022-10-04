{{ config(
    materialized = 'incremental',
    unique_key = 'category_id'
) }}
{{ streamkap_incremental_on_source_to_current(
    source_name = 'catalog_category_product',
    id_field = 'category_id'
) }}
