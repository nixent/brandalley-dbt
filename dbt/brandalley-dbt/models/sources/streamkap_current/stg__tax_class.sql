{{config(
    materialized='incremental',
    unique_key='class_id',
	cluster_by='class_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='tax_class', id_field=config.get('unique_key'))}}