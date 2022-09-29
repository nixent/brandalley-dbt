{{config(
    materialized='incremental',
    unique_by='subscriber_id'
)}}

{{streamkap_incremental_on_source_to_current(source_name='newsletter_subscriber', id_field='subscriber_id')}}