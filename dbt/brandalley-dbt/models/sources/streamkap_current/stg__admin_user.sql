{{config(
    materialized='incremental',
    unique_key= 'user_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='admin_user', id_field=config.get('unique_key'))}}