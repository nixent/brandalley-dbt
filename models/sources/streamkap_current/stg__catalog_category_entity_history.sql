{{config(
    materialized='incremental',
    unique_key='line_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='catalog_category_entity_history', id_field='line_id')}}