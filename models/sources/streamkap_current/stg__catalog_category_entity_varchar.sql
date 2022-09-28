{{config(
    materialized='incremental',
    unique_by='value_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_category_entity_varchar', id_field='value_id')}}