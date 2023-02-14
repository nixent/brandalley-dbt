select
  customer_id,
  magentoID as order_id,
  created_at as order_at,
  status as order_status,
  orderno as order_sequence,
	order_number_excl_full_refunds,
	order_number_incl_cancellations
from {{ ref('orders_incremental') }} 
