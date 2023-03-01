with stock_file_raw as (SELECT 
    STRING_AGG(distinct CAST(category_id AS STRING)) AS parent_child_category_ids,
    e.entity_id AS child_entity_id,
    e.sku AS child_sku,
    stock.min_qty,
    stock.qty,
    IFNULL(parent_relation.parent_id, e.entity_id) AS parent_id,
    parent_entity_relation.sku AS child_parent_sku,
    image.value AS image_value,
    cpvn.value AS name,
    cpevsi.value,
    cpevsiv.sup_id AS suplier_id,
    cpevsiv.name AS supplier_name,
    eaov_brand.value AS brand,
    cpevcoorigin.value AS country_of_manufacture,
    cpedcost.value AS cost,
    REPLACE(REPLACE(REPLACE(cpev_parent_gender.value, '13', 'Female'), '14', 'Male'),'11636','Unisex') AS parent_gender,
    REPLACE(REPLACE(REPLACE(cpev_simple_gender.value, '13', 'Female'), '14', 'Male'),'11636','Unisex') AS simple_gender,
    eaov_simple_type.value AS simple_product_type,
    eaov_parent_type.value AS parent_product_type,
    eaov_size.value AS size,
    eaov_color.value AS colour,
    cpedprice.value AS price,
    cpedsprice.value AS special_price,
    cpedoprice.value AS outlet_price,
    cpev_outlet_category.value AS outlet_category,
    IF(SUM(stock_child.min_qty) < 0,
        'No',
        'Yes') AS canUseForWHSale,
    cpev_barcode.value AS barcode,
    cpev_nego.value AS nego,
    cpn.buyer AS buyer_id,
    CONCAT(au.firstname, ' ', au.lastname) AS buyer,
    SPLIT(cpev_outlet_category.value, '>')[offset(0)] level_1, 
    IF(LENGTH(cpev_outlet_category.value) - LENGTH(REGEXP_REPLACE(cpev_outlet_category.value, '>', ''))>0, SPLIT(cpev_outlet_category.value, '>')[offset(1)], null) level_2, 
    IF(LENGTH(cpev_outlet_category.value) - LENGTH(REGEXP_REPLACE(cpev_outlet_category.value, '>', ''))>1, SPLIT(cpev_outlet_category.value, '>')[offset(2)], null) level_3,
    if(cpei_menu_type_3.value=3, replace(STRING_AGG(distinct category_details.path_name ORDER BY path_name), ',', '\n'), null) as parent_category,
    if(cpei_menu_type_1.value=1, replace(STRING_AGG(distinct category_details.path_name ORDER BY path_name), ',', '\n'), null) as flashsale_category,
FROM
		{{ ref(
				'stg__catalog_product_entity'
		) }}
		e
        INNER JOIN
		{{ ref(
				'stg__cataloginventory_stock_item'
		) }}
		stock ON stock.product_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_super_link'
		) }}
		parent_relation ON parent_relation.product_id = e.entity_id
        INNER JOIN
		{{ ref(
				'stg__catalog_product_entity'
		) }}
		parent_entity_relation ON parent_entity_relation.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_category_product'
		) }}
		category ON category.product_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		image ON image.attribute_id = 85
        AND image.entity_id = e.entity_id
        INNER JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpvn ON cpvn.attribute_id = 71
        AND cpvn.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpevsi ON cpevsi.attribute_id = 239
        AND cpevsi.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_supplier'
		) }}
		cpevsiv ON cpevsi.value = cpevsiv.supplier_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpeib ON cpeib.attribute_id = 178
        AND cpeib.entity_id = parent_relation.parent_id
        LEFT JOIN {{ ref(
           'stg__eav_attribute_option_value'
        ) }}
        eaov_brand
        ON eaov_brand.option_id = cpeib.value
        AND eaov_brand.store_id = 0
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpevcoorigin ON cpevcoorigin.attribute_id = 117
        AND cpevcoorigin.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedcost ON cpedcost.attribute_id = 79
        AND cpedcost.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_parent_gender ON cpev_parent_gender.attribute_id = 180
        AND cpev_parent_gender.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_simple_gender ON cpev_simple_gender.attribute_id = 180
        AND cpev_simple_gender.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_simple_type ON cpev_simple_type.attribute_id = 179
        AND cpev_simple_type.entity_id = e.entity_id
        LEFT JOIN {{ ref(
            'stg__eav_attribute_option_value'
        ) }}
        eaov_simple_type
        ON cpev_simple_type.value = CAST(
                eaov_simple_type.option_id AS STRING
        )
        AND eaov_simple_type.store_id = 0
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_parent_type ON cpev_parent_type.attribute_id = 179
        AND cpev_parent_type.entity_id = parent_relation.parent_id
        LEFT JOIN {{ ref(
            'stg__eav_attribute_option_value'
        ) }}
        eaov_parent_type
        ON cpev_parent_type.value = CAST(
                eaov_parent_type.option_id AS STRING
        )
        AND eaov_simple_type.store_id = 0
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpei_size ON cpei_size.attribute_id = 177
        AND cpei_size.entity_id = e.entity_id
        LEFT JOIN {{ ref(
            'stg__eav_attribute_option_value'
        ) }}
        eaov_size
        ON eaov_size.option_id = cpei_size.value
        AND eaov_size.store_id = 0
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_int'
		) }}
		cpei_colour ON cpei_colour.attribute_id = 213
        AND cpei_colour.entity_id = e.entity_id
        LEFT JOIN {{ ref(
            'stg__eav_attribute_option_value'
        ) }}
        eaov_color
        ON eaov_color.option_id = cpei_colour.value
        AND eaov_color.store_id = 0
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedprice ON cpedprice.attribute_id = 75
        AND cpedprice.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedsprice ON cpedsprice.attribute_id = 76
        AND cpedsprice.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_decimal'
		) }}
		cpedoprice ON cpedoprice.attribute_id = 224
        AND cpedoprice.entity_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_outlet_category ON cpev_outlet_category.attribute_id = 205
        AND cpev_outlet_category.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_super_link'
		) }}
		parent_relation_child ON parent_relation_child.parent_id = parent_relation.parent_id
        LEFT JOIN
		{{ ref(
				'stg__cataloginventory_stock_item'
		) }}
		stock_child ON stock_child.product_id = parent_relation_child.product_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_barcode ON cpev_barcode.attribute_id = 252
        AND cpev_barcode.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_entity_varchar'
		) }}
		cpev_nego ON cpev_nego.attribute_id = 204
        AND cpev_nego.entity_id = e.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_product_negotiation'
		) }}
		cpn ON CAST(cpn.negotiation_id AS STRING) = cpev_nego.value
        LEFT JOIN
        {{ ref(
                'stg__admin_user'
        ) }}
        au ON cpn.buyer = au.user_id
        LEFT JOIN             
		{{ ref(
				'catalog_category_flat_store_1_enriched'
		) }}
		category_details ON category.category_id = category_details.entity_id
        LEFT JOIN
		{{ ref(
				'stg__catalog_category_entity_int'
		) }}
		cpei_menu_type_3 ON cpei_menu_type_3.attribute_id = 373
        AND cpei_menu_type_3.entity_id = category.category_id
        AND cpei_menu_type_3.value=3
        LEFT JOIN
		{{ ref(
				'stg__catalog_category_entity_int'
		) }}
		cpei_menu_type_1 ON cpei_menu_type_1.attribute_id = 373
        AND cpei_menu_type_1.entity_id = category.category_id
        AND cpei_menu_type_1.value=1
        WHERE
    (e.type_id = 'simple')
        AND (stock.qty > 0)
group by 
    e.entity_id,
    e.sku,
    stock.min_qty,
    stock.qty,
    IFNULL(parent_relation.parent_id, e.entity_id),
    parent_entity_relation.sku,
    image.value,
    cpvn.value,
    cpevsi.value,
    cpevsiv.sup_id,
    cpevsiv.name,
    eaov_brand.value,
    cpevcoorigin.value,
    cpedcost.value,
    cpev_parent_gender.value,
    cpev_simple_gender.value,
    eaov_parent_type.value,
    eaov_simple_type.value,
    eaov_size.value,
    eaov_color.value,
    cpedprice.value,
    cpedsprice.value,
    cpedoprice.value,
    cpev_outlet_category.value,
    cpev_barcode.value,
    cpev_nego.value,
    cpn.buyer,
    CONCAT(au.firstname, ' ', au.lastname),
    cpei_menu_type_3.value,
    cpei_menu_type_1.value
 )

select child_entity_id,child_sku,min_qty,qty,string_agg(distinct child_parent_sku) as child_parent_sku,image_value,name,value,
suplier_id,supplier_name,brand,country_of_manufacture,cost,parent_gender,simple_gender,simple_product_type,parent_product_type,
size,colour,price,min(special_price) special_price,outlet_price,outlet_category,canUseForWHSale,barcode,nego,buyer_id,buyer,
level_1,level_2,level_3, string_agg(distinct parent_category) parent_category, string_agg(distinct flashsale_category) flashsale_category,
cat_map.category
from stock_file_raw
        LEFT JOIN
		{{ ref(
				'category_mapping'
		) }}
        cat_map on 
        IF(level_1 is not null, 
            level_1, 
            IF(string_agg(distinct parent_category) is not null, 
                SPLIT(parent_category, '>')[offset(0)], 
                    if(string_agg(distinct flashsale_category) is not null, 
                        SPLIT(flashsale_category, '>')[offset(0)], null)
                )
            ) = cat_map.row_label and 
        IF(level_2 is not null, 
            level_2, 
            IF(LENGTH(string_agg(distinct parent_category)) - LENGTH(REGEXP_REPLACE(string_agg(distinct parent_category), '>', ''))>0, 
                SPLIT(parent_category, '>')[offset(1)], 
                    if(LENGTH(string_agg(distinct flashsale_category)) - LENGTH(REGEXP_REPLACE(string_agg(distinct flashsale_category), '>', ''))>0, 
                        SPLIT(flashsale_category, '>')[offset(1)], null)
                )
            ) = cat_map.level_2 and 
        IF(level_3 is not null, 
            level_3, 
            IF(LENGTH(string_agg(distinct parent_category)) - LENGTH(REGEXP_REPLACE(string_agg(distinct parent_category), '>', ''))>1, 
                SPLIT(parent_category, '>')[offset(2)], 
                    if(LENGTH(string_agg(distinct flashsale_category)) - LENGTH(REGEXP_REPLACE(string_agg(distinct flashsale_category), '>', ''))>1, 
                        SPLIT(flashsale_category, '>')[offset(2)], null)
                )
            ) = cat_map.level_3 
        IF(LENGTH(cpev_outlet_category.value) - LENGTH(REGEXP_REPLACE(cpev_outlet_category.value, '>', ''))>0, SPLIT(cpev_outlet_category.value, '>')[offset(1)], null) level_2, 
        IF(LENGTH(cpev_outlet_category.value) - LENGTH(REGEXP_REPLACE(cpev_outlet_category.value, '>', ''))>1, SPLIT(cpev_outlet_category.value, '>')[offset(2)], null) level_3,

group by child_entity_id,child_sku,min_qty,qty,image_value,name,value,suplier_id,supplier_name,brand,country_of_manufacture,cost,parent_gender,simple_gender,simple_product_type,parent_product_type,size,colour,price,outlet_price,outlet_category,canUseForWHSale,barcode,nego,buyer_id,buyer,level_1,level_2,level_3