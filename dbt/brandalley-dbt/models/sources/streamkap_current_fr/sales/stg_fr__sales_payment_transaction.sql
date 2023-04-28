{{config(
    materialized='incremental',
    unique_key='transaction_id',
	cluster_by='transaction_id',
)}}


{{streamkap_incremental_on_source_to_current(source_name='sales_payment_transaction', id_field=config.get('unique_key'))}}