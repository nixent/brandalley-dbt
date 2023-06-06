with products as (
    select
        {{dbt_utils.generate_surrogate_key(['cpe.entity_id', 'cpe_child.sku', 'cpsl.product_id', 'cpe.ba_site'])}} as unique_id,
        cpe.entity_id                                   as product_id,
        cpe.ba_site,
        timestamp(cpe.created_at)                       as dt_cr,
        cpsl.product_id                                 as variant_product_id,
        cpe.sku,
        cpe_child.sku                                   as variant_sku,
        cpev_sapid.value                                as sap_product_id,
        cpev_name.value                                 as name,
        cpet_desc.value                                 as description,
        eaov_brand.value                                as brand,
        cpev_supplier.value                             as supplier,
        replace(cpev_outletcat.value, 'Outlet/', '')    as outlet_category,
        eaov_pt.value                                   as product_type,
        eaov_availability.value                         as availability,
        cped_outletprice.value                          as outlet_price,
        eaov_color.value                                as color,
        ifnull(eaov_size.value, eaov_size_child.value)  as size,
        if(csi_child.qty > 0, csi_child.qty, csi.qty)   as stock,
        cped_price.value                                as price,
        cped_sprice.value                               as sale_price,
        cped_cost.value                                 as cost,
        tc.class_name                                   as tax_class,
        timestamp(cpe.updated_at)                       as updated_at,
        cps_supplier.sup_id                             as supplier_id,
        cps_supplier.name                               as supplier_name,
        eaov_gender.value                               as gender,
        cpev_barcode.value                              as barcode 
    from {{ ref('stg__catalog_product_entity') }} cpe
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_name
        on cpe.entity_id = cpev_name.entity_id
            and cpev_name.attribute_id = 71
            and cpev_name.store_id = 0
            and cpe.ba_site = cpev_name.ba_site
    left join {{ ref('stg__catalog_product_entity_text') }} cpet_desc
        on cpe.entity_id = cpet_desc.entity_id
            and cpet_desc.attribute_id = 72
            and cpet_desc.store_id = 0
            and cpe.ba_site = cpet_desc.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_sapid
        on cpe.entity_id = cpev_sapid.entity_id
            and cpev_sapid.attribute_id = 223
            and cpev_sapid.store_id = 0
            and cpe.ba_site = cpev_sapid.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_brand
        on cpei_brand.entity_id = cpe.entity_id
            and cpei_brand.attribute_id = 178
            and cpei_brand.store_id = 0
            and cpe.ba_site = cpei_brand.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_brand
        on eaov_brand.option_id = cpei_brand.value
            and eaov_brand.store_id = 0
            and cpe.ba_site = eaov_brand.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_supplier
        on cpev_supplier.entity_id = cpe.entity_id
            and cpev_supplier.attribute_id = 233
            and cpev_supplier.store_id = 0
            and cpe.ba_site = cpev_supplier.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outletcat
        on cpev_outletcat.entity_id = cpe.entity_id
            and cpev_outletcat.attribute_id = 205
            and cpev_outletcat.store_id = 0
            and cpe.ba_site = cpev_outletcat.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pt
        on cpev_pt.entity_id = cpe.entity_id
            and cpev_pt.attribute_id = 179
            and cpev_pt.store_id = 0
            and cpe.ba_site = cpev_pt.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_pt
        on cpev_pt.value = cast(eaov_pt.option_id as string)
            and eaov_pt.store_id = 0
            and cpe.ba_site = eaov_pt.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_availability
        on cpei_availability.entity_id = cpe.entity_id
            and cpei_availability.attribute_id = 195
            and cpei_availability.store_id = 0
            and cpe.ba_site = cpei_availability.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_availability
        on eaov_availability.option_id = cpei_availability.value
            and eaov_availability.store_id = 0
            and cpe.ba_site = eaov_availability.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_color
        on cpei_color.entity_id = cpe.entity_id
            and cpei_color.attribute_id = 213
            and cpei_color.store_id = 0
            and cpe.ba_site = cpei_color.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_color
        on eaov_color.option_id = cpei_color.value
            and eaov_color.store_id = 0
            and cpe.ba_site = eaov_color.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_gender
        on cpei_gender.entity_id = cpe.entity_id
            and cpei_gender.attribute_id = 96
            and cpei_gender.store_id = 0
            and cpe.ba_site = cpei_gender.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_gender
        on eaov_gender.option_id = cpei_gender.value
            and eaov_gender.store_id = 0
            and cpe.ba_site = eaov_gender.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_size
        on cpei_size.entity_id = cpe.entity_id
            and cpei_size.attribute_id = 177
            and cpei_size.store_id = 0
            and cpe.ba_site = cpei_size.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_size
        on eaov_size.option_id = cpei_size.value
            and eaov_size.store_id = 0
            and cpe.ba_site = eaov_size.ba_site
    left join (
		select * from {{ ref('stg__catalog_product_super_link') }}
		qualify row_number() over (partition by product_id, ba_site order by link_id desc) = 1
	) cpsl
        on cpe.entity_id = cpsl.parent_id
        and cpe.ba_site = cpsl.ba_site
    left join {{ ref('stg__catalog_product_entity') }} cpe_child
        on cpsl.product_id = cpe_child.entity_id
        and cpe.ba_site = cpe_child.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_size_child
        on cpei_size_child.entity_id = cpe_child.entity_id
            and cpei_size_child.attribute_id = 177
            and cpei_size_child.store_id = 0
            and cpe.ba_site = cpei_size_child.ba_site
    left join {{ ref('stg__eav_attribute_option_value') }} eaov_size_child
        on eaov_size_child.option_id = cpei_size_child.value
            and eaov_size_child.store_id = 0
            and cpe.ba_site = eaov_size_child.ba_site
    left join {{ ref('stg__cataloginventory_stock_item') }} csi
        on csi.product_id = cpe.entity_id
        and cpe.ba_site = csi.ba_site
    left join {{ ref('stg__cataloginventory_stock_item') }} csi_child
        on csi_child.product_id = cpe_child.entity_id
        and cpe.ba_site = csi_child.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cped_outletprice
        on cpe.entity_id = cped_outletprice.entity_id
            and cped_outletprice.attribute_id = 75
            and cped_outletprice.store_id = 0
            and cpe.ba_site = cped_outletprice.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cped_price
        on cpe.entity_id = cped_price.entity_id
            and cped_price.attribute_id = 75
            and cped_price.store_id = 0
            and cpe.ba_site = cped_price.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cped_sprice
        on cpe.entity_id = cped_sprice.entity_id
            and cped_sprice.attribute_id = 76
            and cped_sprice.store_id = 0
            and cpe.ba_site = cped_sprice.ba_site
    left join {{ ref('stg__catalog_product_entity_decimal') }} cped_cost
        on cpe.entity_id = cped_cost.entity_id
            and cped_cost.attribute_id = 79
            and cped_cost.store_id = 0
            and cpe.ba_site = cped_cost.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_tax
        on cpe.entity_id = cpei_tax.entity_id
            and cpei_tax.attribute_id = 122
            and cpei_tax.store_id = 0
            and cpe.ba_site = cpei_tax.ba_site
    left join {{ ref('stg__tax_class') }} tc
        on cpei_tax.value = tc.class_id
        and cpe.ba_site = tc.ba_site
    left join {{ ref('stg__catalog_product_entity_int') }} cpei_supplier
        on cpei_supplier.entity_id = cpe.entity_id
            and cpei_supplier.attribute_id = 239
            and cpei_supplier.store_id = 0
            and cpe.ba_site = cpei_supplier.ba_site
    left join {{ ref('stg__catalog_product_supplier') }} cps_supplier
        on cpei_supplier.value = cps_supplier.supplier_id
        and cpe.ba_site = cps_supplier.ba_site
    left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_barcode
        on cpe.entity_id = cpev_barcode.entity_id
            and cpev_barcode.attribute_id = 252
            and cpev_barcode.store_id = 0
            and cpe.ba_site = cpev_barcode.ba_site
)

select
    *,
    initcap(split(outlet_category, '>')[safe_offset(0)]) as product_category_level_1, 
	initcap(split(outlet_category, '>')[safe_offset(1)]) as product_category_level_2,
	initcap(split(outlet_category, '>')[safe_offset(2)]) as product_category_level_3
from products