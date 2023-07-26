{% snapshot stock_file_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='ba_site_child_entity_id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('stock_file') }}

{% endsnapshot %}