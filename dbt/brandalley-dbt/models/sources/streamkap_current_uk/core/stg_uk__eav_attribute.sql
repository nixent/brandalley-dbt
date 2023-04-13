{{ config(
    materialized = 'incremental',
    unique_key = 'attribute_id'
) }}

{{streamkap_incremental_on_source_to_current(source_name='eav_attribute', id_field=config.get('unique_key'))}}
