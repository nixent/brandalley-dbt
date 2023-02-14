{{ config(
    materialized = 'incremental',
    unique_key = 'entity_id',
    partition_by = {
      "field": "_streamkap_source_ts_ms",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = 'entity_id',
) }}

{{ streamkap_incremental_on_source_to_current(
    source_name = 'sales_flat_order',
    id_field = config.get('unique_key')
) }}
