{{config(
    materialized='incremental',
    unique_key='value_id',
	cluster_by='value_id',
    partition_by = {
      "field": "bq_last_processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

{{streamkap_incremental_on_source_to_current(source_name='customer_entity_datetime', id_field=config.get('unique_key'))}}