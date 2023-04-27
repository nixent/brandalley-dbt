
select 
    cpe.entity_id,
    cpe.type_id,
    cpe.sku             as sku,
    cpe.ba_site,
    cped_cost.value     as cost,
    eaov_pro_type.value as product_type,
    cpev_barcode.value  as barcode
from {{ ref('stg__catalog_product_entity') }} cpe
left join {{ ref('stg__catalog_product_entity_decimal') }} cped_cost 
    on cpe.entity_id = cped_cost.entity_id
        and cped_cost.attribute_id = 79 
        and cped_cost.store_id = 0
        and cped_cost.ba_site = cpe.ba_site
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pro_type 
    on cpev_pro_type.entity_id = cpe.entity_id 
        and cpev_pro_type.attribute_id = 179
        and cpev_pro_type.ba_site = cpe.ba_site
left join {{ ref('stg__eav_attribute_option_value') }} eaov_pro_type 
    on cast(eaov_pro_type.option_id as string) = cpev_pro_type.value
        and eaov_pro_type.ba_site = cpev_pro_type.ba_site
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_barcode 
    on cpe.entity_id = cpev_barcode.entity_id
        and cpev_barcode.attribute_id = 252 
        and cpev_barcode.store_id = 0
        and cpev_barcode.ba_site = cpe.ba_site