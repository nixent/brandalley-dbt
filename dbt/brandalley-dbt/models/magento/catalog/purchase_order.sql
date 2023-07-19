select  
    poi.po_id, 
    poi.po_item_id, 
    poi.product_id, 
    poi.ba_site,
    poi.sku, 
    poi.reference, 
    poi.simple_name, 
    poi.brand, 
    poi.colour, 
    poi.web_size, 
    poi.supplier_size, 
    poi.cost, 
    poi.cost_gbp, 
    poi.rrp, 
    poi.tax_rate, 
    poi.tax_code, 
    poi.barcode, 
    poi.commodity_code, 
    poi.category, 
    poi.sub_category, 
    poi.sub_sub_category, 
    poi.image, 
    poi.pack_size, 
    poi.to_order, 
    poi.to_order_original, 
    safe_cast(poi.updated_at as timestamp)          as poi_updated, 
    po.negotiation_id, 
    safe_cast(po.created_at as timestamp)           as po_created, 
    safe_cast(po.updated_at as timestamp)           as po_updated, 
    po.status                                       as po_status, 
    po.sap_ref, 
    po.sap_message, 
    safe_cast(po.delivery_date as timestamp)        as po_delivery, 
    safe_cast(po.date_exported as timestamp)        as po_date_exported, 
    po.products_in_wh_b, 
    po.products_export_process_id, 
    po.purchase_order_export_process_id, 
    po.purchase_order_in_wh_b, 
    spg.grn_id, 
    safe_cast(spg.date as timestamp)                as grn_date,
    spg.magento_process_id, 
    spgi.grn_item_id, 
    spgi.in_sap, 
    spgi.delivery_number                            as grn_delivery_number, 
    spgi.delivery_date                              as grn_delivery_date, 
    spgi.stock_type_ss, 
    spgi.stock_type_qc, 
    safe_cast(nego.updated_at as timestamp)         as nego_updated, 
    nego.type                                       as nego_type, 
    nego.supplier, 
    sup.sup_id                                      as supplier_id,
    sup.name                                        as supplier_name,
    nego.currency                                   as nego_currency, 
    nego.buyer, 
    nego.department,
    nego_item.negotiation_item_id,
    nego_item.size,
    nego_item.price, 
    nego_item.qty,			
    nego_item.ordered                               as nego_ordered,
    nego_item.to_order                              as nego_to_order,
    nego_item.qty_exported,
from {{ ref('stg__catalog_product_po_item') }} poi
inner join {{ ref('stg__catalog_product_po') }} po 
    on poi.po_id = po.po_id and poi.ba_site = po.ba_site
left join {{ ref('stg__stock_prism_grn') }} spg 
    on CAST(spg.purchase_order_reference as integer) = po.po_id and po.ba_site = spg.ba_site
left join {{ ref('stg__stock_prism_grn_item') }} spgi 
    on spgi.grn_id = spg.grn_id and spgi.sku = poi.sku and poi.ba_site = spgi.ba_site
left join {{ ref('stg__catalog_product_negotiation') }} nego 
    on po.negotiation_id = nego.negotiation_id and nego.ba_site = po.ba_site
left join {{ ref('stg__catalog_product_negotiation_item') }} nego_item
    on po.negotiation_id = nego_item.negotiation_id and poi.sku=nego_item.sku and nego_item.ba_site = po.ba_site
left join {{ ref('stg__catalog_product_supplier') }} sup
    on nego.supplier = sup.supplier_id and nego.ba_site = sup.ba_site
qualify row_number() over (partition by poi.po_item_id, poi.ba_site order by spgi.delivery_date desc) = 1