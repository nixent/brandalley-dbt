select sha1(concat(sfsi.product_id, sfo.increment_id, ifnull(CAST(sfsi.parent_id AS STRING), '_'), ifnull(CAST(sfoa.entity_id AS STRING), '_'), ifnull(CAST(sfs.order_id AS STRING), '_'), 
ifnull(CAST(sfs.customer_id AS STRING), '_'), ifnull(CAST(sfsi.sku AS STRING), '_'), ifnull(CAST(sfsi.entity_id AS STRING), '_'), ifnull(CAST(sfsi.sap_id AS STRING), '_'))) unique_id, sfsi.product_id, 
sfsi.sku, sfsi.qty, sfsi.weight, sfo.increment_id order_id, sfs.customer_id, sfoa.postcode, sfs.increment_id shipment_id, 
sfo.created_at order_date, ifnull(sfs.created_at, '0000-00-00 00:00:00') shipment_date,sfs.updated_at from {{ source('streamkap', 'sales_flat_shipment_item') }} sfsi 
left join {{ source('streamkap', 'sales_flat_shipment') }} sfs on sfsi.parent_id = sfs.entity_id 
left join {{ source('streamkap', 'sales_flat_order') }} sfo on sfs.order_id = sfo.entity_id 
left join {{ source('streamkap', 'sales_flat_order_address') }} sfoa on sfoa.entity_id = sfo.shipping_address_id  
WHERE (sfo.sales_product_type!=12 or sfo.sales_product_type is null)