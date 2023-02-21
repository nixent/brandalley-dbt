{{ config(
    materialized = 'incremental',
    unique_key = 'entity_id',
    partition_by = {
      "field": "bq_last_processed_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = 'entity_id',
) }}

{{ streamkap_incremental_on_source_to_current(
    source_name = 'sales_flat_order',
    id_field = config.get('unique_key')
) }}
