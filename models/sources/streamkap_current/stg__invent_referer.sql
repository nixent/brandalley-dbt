{{config(
    materialized='incremental',
    unique_by='referer_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='invent_referer', id_field='referer_id')}}