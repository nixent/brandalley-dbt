{{config(
    materialized='incremental',
    unique_by='user_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='admin_user')}}