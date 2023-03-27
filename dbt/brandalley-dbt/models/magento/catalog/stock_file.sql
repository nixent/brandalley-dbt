with stock_file_raw as (SELECT 
    STRING_AGG(distinct CAST(category_id AS STRING)) AS parent_child_category_ids,
    e.entity_id AS child_entity_id,
    e.sku AS child_sku,
    stock.min_qty,
    stock.qty,
    IFNULL(parent_relation.parent_id, e.entity_id) AS parent_id,
    parent_entity_relation.sku AS child_parent_sku,
    if(image.value is not null and image.value!='no_selection', 'https://media.brandalley.co.uk/catalog/product'||image.value,  image.value) AS image_value,
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
    -- Pulling level_1>level_2>level_3 from the outlet_category field
    SPLIT(cpev_outlet_category.value, '>')[offset(0)] level_1, 
    IF(LENGTH(cpev_outlet_category.value) - LENGTH(REGEXP_REPLACE(cpev_outlet_category.value, '>', ''))>0, SPLIT(cpev_outlet_category.value, '>')[offset(1)], null) level_2, 
    IF(LENGTH(cpev_outlet_category.value) - LENGTH(REGEXP_REPLACE(cpev_outlet_category.value, '>', ''))>1, SPLIT(cpev_outlet_category.value, '>')[offset(2)], null) level_3,
    -- Parent category is type 3, flashsale type 1. If there are more than 1 category, we need to put them on the same, but separated with a return carriage character
    if(cpei_menu_type_3.value=3, 
    --STRINGAGG to put multiple lines on one, then 2 replace: 1 for changing separator from comma to return carriage, one to remove the initial 'Root Catalog>Brand Alley UK>' of categories
        replace(
            replace(
                STRING_AGG(distinct category_details.path_name ORDER BY path_name)
            , ',', '\n')
        , 'Root Catalog>Brand Alley UK>', '')
    , null) as parent_category,
--    if(cpei_menu_type_1.value=1, replace(replace(RTRIM(REGEXP_EXTRACT(STRING_AGG(category_details.path_name ORDER BY category_details.created_at), '(?:.*?,){3}'), ','), ',', '\n'), 'Root Catalog>Brand Alley UK>', ''), null) as flashsale_category
    if(cpei_menu_type_1.value=1, 
    --STRINGAGG to put multiple lines on one, categories need to be ordered from oldest to newest
        STRING_AGG(category_details.path_name ORDER BY category_details.created_at)
    , null) as flashsale_category,
    cpni.tax_rate as tax,
    cpei_tax.value as tax_class
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
        AND image.entity_id = parent_entity_relation.entity_id
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
		cpei_tax ON cpei_tax.attribute_id = 122
        AND cpei_tax.entity_id = parent_entity_relation.entity_id
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
        (select distinct negotiation_id, parrent_sku, tax_rate from
		{{ ref(
				'stg__catalog_product_negotiation_item'
		) }})
		cpni ON CAST(cpni.negotiation_id AS STRING) = cpev_nego.value and cpni.parrent_sku = parent_entity_relation.sku
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
    parent_relation.parent_id,
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
    au.firstname,
    au.lastname,
    cpei_menu_type_3.value,
    cpei_menu_type_1.value,
    cpni.tax_rate, cpei_tax.value
 )

select child_entity_id,child_sku,min_qty,qty,string_agg(distinct child_parent_sku) as child_parent_sku,image_value,name,value,
suplier_id,supplier_name,brand,country_of_manufacture,cost,parent_gender,simple_gender,simple_product_type,parent_product_type,
size,colour,price,min(special_price) special_price,outlet_price,outlet_category,canUseForWHSale,barcode,nego,buyer_id,buyer,
-- Not ideal to do a nested query there but couldn't find any other way to get the parent and flashsale categories on the same line
level_1,level_2,level_3, string_agg(parent_category) parent_category, tax, tax_class, replace(
    replace(
        if(LENGTH(string_agg(flashsale_category)) - LENGTH(REGEXP_REPLACE(string_agg(flashsale_category), ',', ''))>=3,
        RTRIM(
            REGEXP_EXTRACT(string_agg(flashsale_category)            
            -- The REGEXP_EXTRACT help to keep only characters up to the 3rd comma as Buying team doesn't want more than 3 categories
            , '(?:.*?,){3}')
        -- RTRIM to remove the last comma
        , ','),
        string_agg(flashsale_category))
    -- replacing commas by return carriage
    , ',', '\n')
-- removing the initial unwanted 'Root Catalog>Brand Alley UK>' categories
, 'Root Catalog>Brand Alley UK>', '') flashsale_category, category from (
select child_entity_id,child_sku,min_qty,qty,string_agg(distinct child_parent_sku) as child_parent_sku,image_value,name,value,
suplier_id,supplier_name,brand,country_of_manufacture,cost,parent_gender,simple_gender,simple_product_type,parent_product_type,
size,colour,price,min(special_price) special_price,outlet_price,outlet_category,canUseForWHSale,barcode,nego,buyer_id,buyer,
stock.level_1,stock.level_2,stock.level_3, 
string_agg(distinct parent_category) parent_category, tax, tax_class,
                (select string_agg(distinct value order by value) from unnest(split(flashsale_category, ',')) as value)
 flashsale_category,
cat_map.category
from stock_file_raw stock
        LEFT JOIN
        -- join on the mapping provided by the buying team
        -- the logic is we look at outlet_category first to do the join. If not possible we use parent_category then flashsale_category.
		{{ source(
            'utils',
            'category_mapping'
        ) }}
        cat_map on 
        -- join on level 1 (First element of path in outlet_category, 3rd element in parent_category and flashsale_category)
        IF(stock.level_1 is not null, 
            stock.level_1, 
            IF(LENGTH(parent_category) - LENGTH(REGEXP_REPLACE(parent_category, '>', ''))>2, 
                SPLIT(parent_category, '>')[offset(2)], 
                    if(flashsale_category is not null, 
                        SPLIT(flashsale_category, '>')[offset(2)], null)
                )
            ) = cat_map.row_label and 
        -- join on level 2 (Second element of path in outlet_category, 4th element in parent_category and flashsale_category)
        IF(stock.level_2 is not null, 
            stock.level_2, 
            IF(LENGTH(parent_category) - LENGTH(REGEXP_REPLACE(parent_category, '>', ''))>3, 
                SPLIT(parent_category, '>')[offset(3)], 
                    if(LENGTH(flashsale_category) - LENGTH(REGEXP_REPLACE(flashsale_category, '>', ''))>0, 
                        SPLIT(flashsale_category, '>')[offset(3)], null)
                )
            ) = cat_map.level_2 and 
        -- join on level 3 (Third element of path in outlet_category, 5th element in parent_category and flashsale_category)
        IF(stock.level_3 is not null, 
            stock.level_3, 
            IF(LENGTH(parent_category) - LENGTH(REGEXP_REPLACE(parent_category, '>', ''))>4, 
                SPLIT(parent_category, '>')[offset(4)], 
                    if(LENGTH(flashsale_category) - LENGTH(REGEXP_REPLACE(flashsale_category, '>', ''))>1, 
                        SPLIT(flashsale_category, '>')[offset(4)], null)
                )
            ) = cat_map.level_3
group by child_entity_id,child_sku,min_qty,qty,image_value,name,value,suplier_id,supplier_name,brand,country_of_manufacture,cost,parent_gender,simple_gender,simple_product_type,parent_product_type,size,colour,price,outlet_price,outlet_category,canUseForWHSale,barcode,nego,buyer_id,buyer,stock.level_1,stock.level_2,stock.level_3, cat_map.category, flashsale_category, tax, tax_class)
group by child_entity_id,child_sku,min_qty,qty,image_value,name,value,suplier_id,supplier_name,brand,country_of_manufacture,cost,parent_gender,simple_gender,simple_product_type,parent_product_type,size,colour,price,outlet_price,outlet_category,canUseForWHSale,barcode,nego,buyer_id,buyer,level_1,level_2,level_3, category, tax, tax_class