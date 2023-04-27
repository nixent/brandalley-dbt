{{config(
    materialized='incremental',
    unique_key='item_id',
	cluster_by='order_id',
    partition_by = {
      "field": "bq_last_processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

{{streamkap_incremental_on_source_to_current(source_name='sales_flat_order_item', id_field=config.get('unique_key'))}}