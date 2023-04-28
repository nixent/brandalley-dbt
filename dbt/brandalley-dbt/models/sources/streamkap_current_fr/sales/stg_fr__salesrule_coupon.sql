{{config(
    materialized='incremental',
    unique_key='coupon_id',
	cluster_by='coupon_id',
)}}

{{streamkap_incremental_on_source_to_current(source_name='salesrule_coupon', id_field=config.get('unique_key'))}}