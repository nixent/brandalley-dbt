SELECT
    SHA1(
        CONCAT (
            cpe.entity_id,
            IFNULL (CAST(eaov_brand.option_id AS STRING), '_'),
            IFNULL (CAST(cpev_pt.value AS STRING), '_'),
            IFNULL (CAST(eaov_availability.option_id AS STRING), '_'),
            IFNULL (CAST(eaov_color.option_id AS STRING), '_'),
            IFNULL (CAST(eaov_size.option_id AS STRING), '_'),
            IFNULL (CAST(cpe_child.entity_id AS STRING), '_'),
            IFNULL (CAST(eaov_size_child.option_id AS STRING), '_'),
            IFNULL (CAST(cpei_tax.value AS STRING), '_')
        )
    ) AS u_unique_id,
    cpe.entity_id AS product_id,
    cpe.created_at AS dt_cr,
    cpsl.product_id AS variant_product_id,
    cpe.sku,
    cpe_child.sku AS variant_sku,
    cpev_sapid.value AS sap_product_id,
    cpev_name.value AS NAME,
    cpet_desc.value AS description,
    eaov_brand.value AS brand,
    cpev_supplier.value AS supplier,
    REPLACE(
        cpev_outletcat.value,
        'Outlet/',
        ''
    ) AS outlet_category,
    eaov_pt.value AS product_type,
    eaov_availability.value AS availability,
    cped_outletprice.value AS outlet_price,
    eaov_color.value AS color,
    IFNULL(
        eaov_size.value,
        eaov_size_child.value
    ) AS SIZE,
    IF (
        csi_child.qty > 0,
        csi_child.qty,
        csi.qty
    ) AS stock,
    cped_price.value AS price,
    cped_sprice.value AS sale_price,
    cped_cost.value AS cost,
    tc.class_name AS tax_class,
    cpe.updated_at,
    cps_supplier.sup_id AS supplier_id,
    cps_supplier.name AS supplier_name,
    eaov_gender.value AS gender
FROM
    {{ ref(
        'stg__catalog_product_entity'
    ) }}
    cpe
    LEFT JOIN     {{ ref(
        'stg__catalog_product_entity_varchar'
    ) }}
    cpev_name
    ON cpe.entity_id = cpev_name.entity_id
    AND cpev_name.attribute_id = 71
    AND cpev_name.store_id = 0
    LEFT JOIN     {{ ref(
        'stg__catalog_product_entity_text'
    ) }}
    cpet_desc
    ON cpe.entity_id = cpet_desc.entity_id
    AND cpet_desc.attribute_id = 72
    AND cpet_desc.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_varchar'
    ) }}
    cpev_sapid
    ON cpe.entity_id = cpev_sapid.entity_id
    AND cpev_sapid.attribute_id = 223
    AND cpev_sapid.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_brand
    ON cpei_brand.entity_id = cpe.entity_id
    AND cpei_brand.attribute_id = 178
    AND cpei_brand.store_id = 0
    LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value'
    ) }}
    eaov_brand
    ON eaov_brand.option_id = cpei_brand.value
    AND eaov_brand.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_varchar'
    ) }}
    cpev_supplier
    ON cpev_supplier.entity_id = cpe.entity_id
    AND cpev_supplier.attribute_id = 233
    AND cpev_supplier.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_varchar'
    ) }}
    cpev_outletcat
    ON cpev_outletcat.entity_id = cpe.entity_id
    AND cpev_outletcat.attribute_id = 205
    AND cpev_outletcat.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_varchar'
    ) }}
    cpev_pt
    ON cpev_pt.entity_id = cpe.entity_id
    AND cpev_pt.attribute_id = 179
    AND cpev_pt.store_id = 0
    LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value'
    ) }}
    eaov_pt
    ON cpev_pt.value = CAST(
        eaov_pt.option_id AS STRING
    )
    AND eaov_pt.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_availability
    ON cpei_availability.entity_id = cpe.entity_id
    AND cpei_availability.attribute_id = 195
    AND cpei_availability.store_id = 0
    LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value'
    ) }}
    eaov_availability
    ON eaov_availability.option_id = cpei_availability.value
    AND eaov_availability.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_color
    ON cpei_color.entity_id = cpe.entity_id
    AND cpei_color.attribute_id = 213
    AND cpei_color.store_id = 0
    LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value'
    ) }}
    eaov_color
    ON eaov_color.option_id = cpei_color.value
    AND eaov_color.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_gender
    ON cpei_gender.entity_id = cpe.entity_id
    AND cpei_gender.attribute_id = 96
    AND cpei_gender.store_id = 0
    LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value'
    ) }}
    eaov_gender
    ON eaov_gender.option_id = cpei_gender.value
    AND eaov_gender.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_size
    ON cpei_size.entity_id = cpe.entity_id
    AND cpei_size.attribute_id = 177
    AND cpei_size.store_id = 0
    LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value'
    ) }}
    eaov_size
    ON eaov_size.option_id = cpei_size.value
    AND eaov_size.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_super_link'
    ) }}
    cpsl
    ON cpe.entity_id = cpsl.parent_id
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity'
    ) }}
    cpe_child
    ON cpsl.product_id = cpe_child.entity_id
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_size_child
    ON cpei_size_child.entity_id = cpe_child.entity_id
    AND cpei_size_child.attribute_id = 177
    AND cpei_size_child.store_id = 0
    LEFT JOIN {{ ref(
        'stg__eav_attribute_option_value'
    ) }}
    eaov_size_child
    ON eaov_size_child.option_id = cpei_size_child.value
    AND eaov_size_child.store_id = 0
    LEFT JOIN {{ ref(
        'stg__cataloginventory_stock_item'
    ) }}
    csi
    ON csi.product_id = cpe.entity_id
    LEFT JOIN {{ ref(
        'stg__cataloginventory_stock_item'
    ) }}
    csi_child
    ON csi_child.product_id = cpe_child.entity_id
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_decimal'
    ) }}
    cped_outletprice
    ON cpe.entity_id = cped_outletprice.entity_id
    AND cped_outletprice.attribute_id = 75
    AND cped_outletprice.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_decimal'
    ) }}
    cped_price
    ON cpe.entity_id = cped_price.entity_id
    AND cped_price.attribute_id = 75
    AND cped_price.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_decimal'
    ) }}
    cped_sprice
    ON cpe.entity_id = cped_sprice.entity_id
    AND cped_sprice.attribute_id = 76
    AND cped_sprice.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_decimal'
    ) }}
    cped_cost
    ON cpe.entity_id = cped_cost.entity_id
    AND cped_cost.attribute_id = 79
    AND cped_cost.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_tax
    ON cpe.entity_id = cpei_tax.entity_id
    AND cpei_tax.attribute_id = 122
    AND cpei_tax.store_id = 0
    LEFT JOIN {{ ref(
        'stg__tax_class'
    ) }}
    tc
    ON cpei_tax.value = tc.class_id
    LEFT JOIN {{ ref(
        'stg__catalog_product_entity_int'
    ) }}
    cpei_supplier
    ON cpei_supplier.entity_id = cpe.entity_id
    AND cpei_supplier.attribute_id = 239
    AND cpei_supplier.store_id = 0
    LEFT JOIN {{ ref(
        'stg__catalog_product_supplier'
    ) }}
    cps_supplier
    ON cpei_supplier.value = cps_supplier.supplier_id
