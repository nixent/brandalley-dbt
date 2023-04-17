select 
    cpn.date_comp_exported  as po_date, 
    cpn.negotiation_id, 
    cpn.ba_site,
    sfoi.sku, 
    sfo.increment_id        as order_id, 
    sfoi.qty_backordered    as consigment_ordered, 
    sfoi.created_at,
    eaov.value              as brand, 
    cpev.value              as name, 
    cpr.reference           as reference_ref, 
    cpr.supplier_id, 
    cpn.sap_ref
from {{ ref('stg__catalog_product_negotiation') }} cpn
inner join {{ ref('stg__sales_flat_order_item') }} sfoi 
    on cpn.negotiation_id = sfoi.nego 
        and sfoi.created_at > cpn.date_comp_exported 
        and sfoi.product_type = 'simple' 
        and cpn.status = 70 
        and sfoi.qty_backordered > 0
        and sfoi.ba_site = cpn.ba_site
join {{ ref('stg__sales_flat_order')}} sfo 
    on sfo.entity_id = sfoi.order_id and sfo.ba_site = sfoi.ba_site
left join {{ ref('stg__catalog_product_reference') }} cpr 
    on cpr.entity_id = sfoi.product_id and sfoi.ba_site = cpr.ba_site
left join {{ ref('stg__sales_flat_order_item') }} sfoii 
    on sfoi.parent_item_id = sfoii.parent_item_id and sfoi.ba_site = sfoii.ba_site
left join {{ ref('stg__catalog_product_entity_int') }} cpei 
    on sfoii.product_id = cpei.entity_id 
        and cpei.attribute_id = 178
        and sfoii.ba_site = cpei.ba_site
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev 
    on sfoii.product_id = cpev.entity_id 
        and cpev.attribute_id = 71
        and sfoii.ba_site = cpev.ba_site
left join {{ ref('stg__eav_attribute_option_value') }} eaov 
    on cpei.value = eaov.option_id and cpei.ba_site = eaov.ba_site