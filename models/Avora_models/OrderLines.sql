SELECT
       SHA1(
              CONCAT(
                     sfoi_con.product_id,
                     sfoi_con.order_id,
                     sfoi_con.item_id,
                     sfo.entity_id,
                     IFNULL (CAST(cpev_pt_con.value AS STRING), '_'),
                     IFNULL (CAST(eaov_brand.option_id AS STRING), '_'),
                     IFNULL (CAST(eaov_color.option_id AS STRING), '_'),
                     IFNULL (CAST(eaov_size.option_id AS STRING), '_'),
                     IFNULL (CAST(cpei_size_child.entity_id AS STRING), '_'),
                     IFNULL (CAST(eaov_size_child.option_id AS STRING), '_')
              )
       ) AS unique_id,
       sfo.increment_id AS order_number,
       sfo.customer_id AS customer_id,
       sfoi_sim.item_id AS order_item_id,
       sfo.entity_id AS order_id,
       sfoi_sim.sku,
       IF (
              au.user_id IS NOT NULL,
              CONCAT(
                     au.firstname,
                     ' ',
                     au.lastname
              ),
              'Unknown'
       ) AS buyer,
       sfoi_con.name,
       sfoi_con.qty_canceled,
       sfoi_sim.qty_ordered,
       sfoi_sim.qty_invoiced,
       sfoi_con.qty_refunded,
       sfoi_con.qty_shipped,
       IF (
              sfoi_sim.qty_backordered IS NULL,
              0,
              sfoi_sim.qty_backordered
       ) AS consignment_qty,
       IF (
              sfoi_sim.qty_backordered IS NULL,
              sfoi_sim.qty_ordered,
              sfoi_sim.qty_ordered - sfoi_sim.qty_backordered
       ) AS warehouse_qty,
       sfo.created_at AS order_placed_date,
       CASE
              WHEN sfoi_con.dispatch_date = CAST(
                     '0000-00-00' AS DATE
              ) THEN NULL
              ELSE sfoi_con.dispatch_date
       END AS dispatch_due_date,
       sfoi_sim.base_cost AS product_cost_inc_vat,
       (
              sfoi_sim.base_cost * sfoi_sim.qty_ordered
       ) AS line_product_cost_inc_vat,
       CAST((sfoi_sim.base_cost) AS DECIMAL) AS product_cost_exc_vat,
       (
              sfoi_sim.base_cost
       ) * sfoi_sim.qty_ordered AS line_product_cost_exc_vat,
       sfoi_con.original_price AS flash_price_inc_vat,
       sfoi_con.original_price * sfoi_sim.qty_ordered AS line_flash_price_inc_vat,
       sfoi_con.original_price /(1 +(sfoi_con.tax_percent / 100.)) AS flash_price_exc_vat,
       (
              sfoi_con.original_price /(
                     1 + sfoi_con.tax_percent / 100.
              )
       ) * sfoi_sim.qty_ordered AS line_flash_price_exc_vat,
       sfoi_con.discount_amount AS line_discount_amount,
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) /((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) AS DECIMAL
       ) AS margin_inc_discount_percentage,
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) AS DECIMAL
       ) AS margin_inc_discount_value,
       CAST(
              (
                     (
                            ((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /(1 +(sfoi_con.tax_percent / 100.))
                     ) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) /((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) AS DECIMAL
       ) AS margin_exc_discount_percentage,
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /(1 +(sfoi_con.tax_percent / 100.))
              ) -(
                     sfoi_sim.base_cost * sfoi_sim.qty_ordered
              ) AS DECIMAL
       ) AS margin_exc_discount_value,
       IF (
              sfo.total_refunded IS NULL,
              0,
              sfo.total_refunded
       ) AS line_total_refunded,
       IF (
              sfo.shipping_refunded IS NULL,
              0,
              sfo.shipping_refunded
       ) AS shipping_refunded,
       IF (
              cpev_outletcat_con.value IS NOT NULL,
              cpev_outletcat_con.value,
              cpev_outletcat_sim.value
       ) AS category_path,
       IF (
              eaov_pt_con.value IS NOT NULL,
              eaov_pt_con.value,
              eaov_pt_sim.value
       ) AS product_type,
       eaov_brand.value AS brand,
       cps_supplier.sup_id AS supplier_id,
       cps_supplier.name AS supplier_name,
       eaov_color.value AS colour,
       IFNULL(
              eaov_size.value,
              eaov_size_child.value
       ) AS SIZE,
       sfoi_con.nego,
       CASE
              WHEN (
                     cceh.name IN (
                            '',
                            'Women',
                            'Men',
                            'Kids',
                            'Lingerie',
                            'Home',
                            'Beauty',
                            'Z_NoData',
                            'Archieved outlet products',
                            'Holding review'
                     )
                     OR cceh.name IS NULL
              ) THEN 'Outlet'
              ELSE cceh.name
       END AS category_name,
       CASE
              cceh.type
              WHEN 1 THEN IF (
                     regexp_contains(
                            LOWER(
                                   IF (
                                          eaov_pt_con.value IS NOT NULL,
                                          eaov_pt_con.value,
                                          eaov_pt_sim.value
                                   )
                            ),
                            r 'Handbags|Purses|Belts|Umbrellas|Hats|Face Coverings|Fashion Hair Accessories|Gloves|Scarves|Jewellery|Tech Accessories|Sunglasses|Travel|Watches|Cosmetics Cases|Bags|Cufflinks & Tie Clips|Ties & Pocket Squares|Wallets|Tech|Accessories|Ski Accessories'
                     ),
                     'ACCESSORIES',
                     IF (
                            regexp_contains(
                                   LOWER(
                                          IF (
                                                 eaov_pt_con.value IS NOT NULL,
                                                 eaov_pt_con.value,
                                                 eaov_pt_sim.value
                                          )
                                   ),
                                   r 'Moisturisers|Cleansers|Anti-Ageing|Toners|Masks|Eye Care|Organic & Natural|Serums|Gift Sets|Treatments|Mens Skincare|Bath & Shower|Hand & Foot Care|Bronzers & Sun Care|Body Sculpting & Toning|Mens Bath & Body|Toothbrushes|Teeth Whitening|Face|Eyes|Lips|Makeup Sets|Nails|Makeup Removers|Womens Perfume|Mens Cologne|Shampoo & Conditioners|Hair Oils & Treatments|Hair Styling & Finishing|Hair Accessories|Personal Care Electricals|Mens Hair Care'
                            ),
                            'BEAUTY',
                            IF (
                                   regexp_contains(
                                          LOWER(
                                                 IF (
                                                        eaov_pt_con.value IS NOT NULL,
                                                        eaov_pt_con.value,
                                                        eaov_pt_sim.value
                                                 )
                                          ),
                                          r 'Ankle Boots|Long Boots|Flat Shoes|Heeled Shoes|Court Shoes|Espadrilles|Pumps|Flat Sandals|Flip Flops|Heeled Sandals|Slippers|Trainers|Boots|Casual Shoes|Formal Shoes|Sandals|Footwear'
                                   ),
                                   'FOOTWEAR',
                                   IF (
                                          regexp_contains(
                                                 LOWER(
                                                        IF (
                                                               eaov_pt_con.value IS NOT NULL,
                                                               eaov_pt_con.value,
                                                               eaov_pt_sim.value
                                                        )
                                                 ),
                                                 r 'Duvets & Pillows|Towels|Bathroom Accessories|Bathroom Fixtures|Bed Accessories|Bed Linen|Mattress Toppers & Protectors|Bath Mats|Beach Towels|Beds|Mattresses|Bookcases Shelving Units & Shelves|Sofas & Armchairs|Coffee Tables|Side Tables|Console Tables|Media Cabinets|Cabinets & Sideboards|Bar Chairs & Stools|Cots & Beds|Seats Tables & Loungers|Barbeque Accessories|Garden Tools|Outdoor Garden Lighting|Planters Pots & Ornaments|Lighting|Candles & Home Fragrance|Curtains & Blinds|Cushions & Throws|Rugs|Wall Art|Mirrors|Decorative Accessories|Stationery|Storage|Pet Care|Pots & Pans|Food Preparation|Utensils|Cooking & Baking|Kitchen Fixtures|Kitchen Storage|Tableware|Drinkware|Cutlery|Barware & Drinks Accessories|Electricals|Laundry & Ironing|Food & Drink|Decorations|Nutrition Supplements|Sports Equipment|Luggage & Suitcases|Backpacks & Holdalls|Travel Accessories|Alcohol|Chocolate & Sweets|Gift Hampers|Games & Puzzles|Baby & Toddler Toys|Childrens Toys|Clothing & Accessories|Gift Sets|Home Gifts|Garden Gifts|Trees|Lights|Bedding|Candles & Fragrance|Cards & Calendars|Home Decor|Wreaths & Garlands|Puddings Chocolates & Sweets|BBQs & Accessories|Outdoor Tableware & Serveware|Garden Furniture Sets|Garden Seating|Garden Tables|Sun Loungers & Swing Seats|Parasols & Accessories|Garden Sheds & Workshops|Summerhouses & Outbuildings|Gazebos Arbours & Arches|Garden Storage|Patio Heaters Fire Pits & Chimineas|Outdoor Wall & Security Lights|Dining Tables & Chairs|Portable & Party Lights|Solar Lights|Decorative Garden Accessories|Outdoor Cushions|Pots & Planters|Outdoor Lanterns|Gardening Tools|Outdoor Toys & Games|Drink'
                                          ),
                                          'HOME',
                                          IF (
                                                 regexp_contains(
                                                        LOWER(
                                                               IF (
                                                                      eaov_pt_con.value IS NOT NULL,
                                                                      eaov_pt_con.value,
                                                                      eaov_pt_sim.value
                                                               )
                                                        ),
                                                        r 'Baby|Boys Clothing|Girls Clothing|Baby Shoes|Boys Shoes|Girls Shoes|Games & Puzzles|Baby & Toddler Toys|Childrens Toys|Childrens Books|Baby Gifts|Buggies & Travel|Nursery Accessories'
                                                 ),
                                                 'KIDS',
                                                 IF (
                                                        regexp_contains(
                                                               LOWER(
                                                                      IF (
                                                                             eaov_pt_con.value IS NOT NULL,
                                                                             eaov_pt_con.value,
                                                                             eaov_pt_sim.value
                                                                      )
                                                               ),
                                                               r 'Bras|Briefs|Bodies|Slips|Nightwear|Shapewear|Suspenders|Socks & Tights|Swimwear & Beachwear'
                                                        ),
                                                        'LINGERIE',
                                                        IF (
                                                               regexp_contains(
                                                                      LOWER(
                                                                             IF (
                                                                                    eaov_pt_con.value IS NOT NULL,
                                                                                    eaov_pt_con.value,
                                                                                    eaov_pt_sim.value
                                                                             )
                                                                      ),
                                                                      r 'Activewear|Blouses & Tops|Coats|Jeans|Dresses|Jackets|Jumpsuits|Knitwear|Leather|Loungewear & Onesies|Maternity|Shorts|Skirts|Sweatshirts & Fleeces|Shirts|Polo Shirts|Suits|Swimwear|Trousers|T-Shirts & Vests|Nightwear|Shorts|Underwear & Socks|Sweatshirts & Hoodies|Team Merchandise|T-Shirts|Track Pants|Vests|Ski Jackets|Ski Trousers|Sports Bras|Outerwear|Leggings|Base Layers'
                                                               ),
                                                               'RTW',
                                                               'OUTLET'
                                                        )
                                                 )
                                          )
                                   )
                            )
                     )
              ) --FIND_IN_SET(IF (eaov_pt_con.value IS NOT NULL,eaov_pt_con.value,eaov_pt_sim.value),'Handbags,Purses,Belts,Umbrellas,Hats,Face Coverings,Fashion Hair Accessories,Gloves,Scarves,Jewellery,Tech Accessories,Sunglasses,Travel,Watches,Cosmetics Cases,Bags,Cufflinks & Tie Clips,Ties & Pocket Squares,Wallets,Tech,Accessories,Ski Accessories') > 0,'ACCESSORIES',IF (FIND_IN_SET(IF (eaov_pt_con.value IS NOT NULL,eaov_pt_con.value,eaov_pt_sim.value),'Moisturisers,Cleansers,Anti-Ageing,Toners,Masks,Eye Care,Organic & Natural,Serums,Gift Sets,Treatments,Mens Skincare,Bath & Shower,Hand & Foot Care,Bronzers & Sun Care,Body Sculpting & Toning,Mens Bath & Body,Toothbrushes,Teeth Whitening,Face,Eyes,Lips,Makeup Sets,Nails,Makeup Removers,Womens Perfume,Mens Cologne,Shampoo & Conditioners,Hair Oils & Treatments,Hair Styling & Finishing,Hair Accessories,Personal Care Electricals,Mens Hair Care') > 0,'BEAUTY',IF (FIND_IN_SET(IF (eaov_pt_con.value IS NOT NULL,eaov_pt_con.value,eaov_pt_sim.value),'Ankle Boots,Long Boots,Flat Shoes,Heeled Shoes,Court Shoes,Espadrilles,Pumps,Flat Sandals,Flip Flops,Heeled Sandals,Slippers,Trainers,Boots,Casual Shoes,Formal Shoes,Sandals,Footwear') > 0,'FOOTWEAR',IF (FIND_IN_SET(IF (eaov_pt_con.value IS NOT NULL,eaov_pt_con.value,eaov_pt_sim.value),'Duvets & Pillows,Towels,Bathroom Accessories,Bathroom Fixtures,Bed Accessories,Bed Linen,Mattress Toppers & Protectors,Bath Mats,Beach Towels,Beds,Mattresses,Bookcases Shelving Units & Shelves,Sofas & Armchairs,Coffee Tables,Side Tables,Console Tables,Media Cabinets,Cabinets & Sideboards,Bar Chairs & Stools,Cots & Beds,Seats Tables & Loungers,Barbeque Accessories,Garden Tools,Outdoor Garden Lighting,Planters Pots & Ornaments,Lighting,Candles & Home Fragrance,Curtains & Blinds,Cushions & Throws,Rugs,Wall Art,Mirrors,Decorative Accessories,Stationery,Storage,Pet Care,Pots & Pans,Food Preparation,Utensils,Cooking & Baking,Kitchen Fixtures,Kitchen Storage,Tableware,Drinkware,Cutlery,Barware & Drinks Accessories,Electricals,Laundry & Ironing,Food & Drink,Decorations,Nutrition Supplements,Sports Equipment,Luggage & Suitcases,Backpacks & Holdalls,Travel Accessories,Alcohol,Chocolate & Sweets,Gift Hampers,Games & Puzzles,Baby & Toddler Toys,Childrens Toys,Clothing & Accessories,Gift Sets,Home Gifts,Garden Gifts,Trees,Lights,Bedding,Candles & Fragrance,Cards & Calendars,Home Decor,Wreaths & Garlands,Puddings Chocolates & Sweets,BBQs & Accessories,Outdoor Tableware & Serveware,Garden Furniture Sets,Garden Seating,Garden Tables,Sun Loungers & Swing Seats,Parasols & Accessories,Garden Sheds & Workshops,Summerhouses & Outbuildings,Gazebos Arbours & Arches,Garden Storage,Patio Heaters Fire Pits & Chimineas,Outdoor Wall & Security Lights,Dining Tables & Chairs,Portable & Party Lights,Solar Lights,Decorative Garden Accessories,Outdoor Cushions,Pots & Planters,Outdoor Lanterns,Gardening Tools,Outdoor Toys & Games,Drink') > 0,'HOME',IF (FIND_IN_SET(IF (eaov_pt_con.value IS NOT NULL,eaov_pt_con.value,eaov_pt_sim.value),'Baby,Boys Clothing,Girls Clothing,Baby Shoes,Boys Shoes,Girls Shoes,Games & Puzzles,Baby & Toddler Toys,Childrens Toys,Childrens Books,Baby Gifts,Buggies & Travel,Nursery Accessories') > 0,'KIDS',IF (FIND_IN_SET(IF (eaov_pt_con.value IS NOT NULL,eaov_pt_con.value,eaov_pt_sim.value),'Bras,Briefs,Bodies,Slips,Nightwear,Shapewear,Suspenders,Socks & Tights,Swimwear & Beachwear') > 0,'LINGERIE',IF (FIND_IN_SET(IF (eaov_pt_con.value IS NOT NULL,eaov_pt_con.value,eaov_pt_sim.value),'Activewear,Blouses & Tops,Coats,Jeans,Dresses,Jackets,Jumpsuits,Knitwear,Leather,Loungewear & Onesies,Maternity,Shorts,Skirts,Sweatshirts & Fleeces,Shirts,Polo Shirts,Suits,Swimwear,Trousers,T-Shirts & Vests,Nightwear,Shorts,Underwear & Socks,Sweatshirts & Hoodies,Team Merchandise,T-Shirts,Track Pants,Vests,Ski Jackets,Ski Trousers,Sports Bras,Outerwear,Leggings,Base Layers') <> 0,'RTW','OUTLET')))))))
              WHEN 2 THEN 'CLEARANCE'
              WHEN 3 THEN 'OUTLET'
              WHEN NULL THEN 'OTHERS'
              ELSE 'OUTLET'
       END AS department_type,
       sfo.updated_at,
       sfo.created_at,
       CAST(
              NULL AS datetime
       ) month_created,
       MAX(
              cpe.sku
       ) AS parent_sku,
       cped_price.value AS rrp,
       MAX(
              cpr.reference
       ) AS REFERENCE,
       sfo.status AS order_status,
       sfoi_con.tax_amount,
       sfoi_con.tax_percent
FROM
       {{ source(
              'streamkap',
              'sales_flat_order'
       ) }}
       sfo
       LEFT JOIN {{ source(
              'streamkap',
              'sales_flat_order_item'
       ) }}
       sfoi_sim
       ON sfoi_sim.order_id = sfo.entity_id
       AND sfoi_sim.product_type = 'simple'
       LEFT JOIN {{ source(
              'streamkap',
              'sales_flat_order_item'
       ) }}
       sfoi_con
       ON sfoi_con.order_id = sfo.entity_id
       AND IF (
              sfoi_sim.parent_item_id IS NOT NULL,
              sfoi_con.item_id = sfoi_sim.parent_item_id,
              sfoi_con.item_id = sfoi_sim.item_id
       )
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_varchar'
       ) }}
       cpev_outletcat_con
       ON cpev_outletcat_con.entity_id = sfoi_con.product_id
       AND cpev_outletcat_con.attribute_id = 205
       AND cpev_outletcat_con.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_varchar'
       ) }}
       cpev_outletcat_sim
       ON cpev_outletcat_sim.entity_id = sfoi_sim.product_id
       AND cpev_outletcat_sim.attribute_id = 205
       AND cpev_outletcat_sim.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_varchar'
       ) }}
       cpev_pt_sim
       ON cpev_pt_sim.entity_id = sfoi_sim.product_id
       AND cpev_pt_sim.attribute_id = 179
       AND cpev_pt_sim.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'eav_attribute_option_value'
       ) }}
       eaov_pt_sim
       ON cpev_pt_sim.value = CAST(
              eaov_pt_sim.option_id AS STRING
       )
       AND eaov_pt_sim.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_varchar'
       ) }}
       cpev_pt_con
       ON cpev_pt_con.entity_id = sfoi_con.product_id
       AND cpev_pt_con.attribute_id = 179
       AND cpev_pt_con.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'eav_attribute_option_value'
       ) }}
       eaov_pt_con
       ON cpev_pt_con.value = CAST(
              eaov_pt_con.option_id AS STRING
       )
       AND eaov_pt_con.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_int'
       ) }}
       cpei_brand
       ON cpei_brand.entity_id = sfoi_con.product_id
       AND cpei_brand.attribute_id = 178
       AND cpei_brand.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'eav_attribute_option_value'
       ) }}
       eaov_brand
       ON eaov_brand.option_id = cpei_brand.value
       AND eaov_brand.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_int'
       ) }}
       cpei_color
       ON cpei_color.entity_id = sfoi_con.product_id
       AND cpei_color.attribute_id = 213
       AND cpei_color.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'eav_attribute_option_value'
       ) }}
       eaov_color
       ON eaov_color.option_id = cpei_color.value
       AND eaov_color.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_int'
       ) }}
       cpei_size
       ON cpei_size.entity_id = sfoi_con.product_id
       AND cpei_size.attribute_id = 177
       AND cpei_size.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'eav_attribute_option_value'
       ) }}
       eaov_size
       ON eaov_size.option_id = cpei_size.value
       AND eaov_size.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_int'
       ) }}
       cpei_size_child
       ON cpei_size_child.entity_id = sfoi_sim.product_id
       AND cpei_size_child.attribute_id = 177
       AND cpei_size_child.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'eav_attribute_option_value'
       ) }}
       eaov_size_child
       ON eaov_size_child.option_id = cpei_size_child.value
       AND eaov_size_child.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_int'
       ) }}
       cpei_supplier
       ON cpei_supplier.entity_id = sfoi_sim.product_id
       AND cpei_supplier.attribute_id = 239
       AND cpei_supplier.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_supplier'
       ) }}
       cps_supplier
       ON cpei_supplier.value = cps_supplier.supplier_id
       LEFT JOIN {{ source(
              'streamkap',
              'sales_flat_order_item_extra'
       ) }}
       sfoie
       ON sfoi_con.item_id = sfoie.order_item_id
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_category_entity_history'
       ) }}
       cceh
       ON sfoie.category_id = cceh.category_id
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_negotiation'
       ) }}
       cpn
       ON sfoi_sim.nego = cpn.negotiation_id
       LEFT JOIN {{ source(
              'streamkap',
              'admin_user'
       ) }}
       au
       ON cpn.buyer = au.user_id
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_super_link'
       ) }}
       cpsl
       ON sfoi_sim.product_id = cpsl.product_id
       LEFT OUTER JOIN {{ source(
              'streamkap',
              'catalog_product_entity'
       ) }}
       cpe
       ON cpe.entity_id = cpsl.parent_id
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity_decimal'
       ) }}
       cped_price
       ON sfoi_sim.product_id = cped_price.entity_id
       AND cped_price.attribute_id = 75
       AND cped_price.store_id = 0
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_entity'
       ) }}
       cpe_ref
       ON sfoi_sim.sku = cpe_ref.sku
       LEFT JOIN {{ source(
              'streamkap',
              'catalog_product_reference'
       ) }}
       cpr
       ON cpe_ref.entity_id = cpr.entity_id
WHERE
       sfo.increment_id NOT LIKE '%-%'
       AND (
              sfo.sales_product_type != 12
              OR sfo.sales_product_type IS NULL
       )
GROUP BY
       unique_id,
       sfo.increment_id,
       sfo.customer_id,
       sfoi_sim.item_id,
       sfo.entity_id,
       sfoi_sim.sku,
       IF (
              au.user_id IS NOT NULL,
              CONCAT(
                     au.firstname,
                     ' ',
                     au.lastname
              ),
              'Unknown'
       ),
       sfoi_con.name,
       sfoi_con.qty_canceled,
       sfoi_sim.qty_ordered,
       sfoi_sim.qty_invoiced,
       sfoi_con.qty_refunded,
       sfoi_con.qty_shipped,
       IF (
              sfoi_sim.qty_backordered IS NULL,
              0,
              sfoi_sim.qty_backordered
       ),
       IF (
              sfoi_sim.qty_backordered IS NULL,
              sfoi_sim.qty_ordered,
              sfoi_sim.qty_ordered - sfoi_sim.qty_backordered
       ),
       sfo.created_at,
       dispatch_due_date,
       sfoi_sim.base_cost,
       (
              sfoi_sim.base_cost * sfoi_sim.qty_ordered
       ),
       CAST((sfoi_sim.base_cost) AS DECIMAL),
       (
              sfoi_sim.base_cost
       ) * sfoi_sim.qty_ordered,
       sfoi_con.original_price,
       sfoi_con.original_price * sfoi_sim.qty_ordered,
       sfoi_con.original_price /(1 +(sfoi_con.tax_percent / 100.)),
       (
              sfoi_con.original_price /(1 +(sfoi_con.tax_percent / 100.))
       ) * sfoi_sim.qty_ordered,
       sfoi_con.discount_amount,
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) /((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) AS DECIMAL
       ),
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) AS DECIMAL
       ),
       CAST(
              (
                     (
                            ((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /(1 +(sfoi_con.tax_percent / 100.))
                     ) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) /((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))) AS DECIMAL
       ),
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /(1 +(sfoi_con.tax_percent / 100.))
              ) -(
                     sfoi_sim.base_cost * sfoi_sim.qty_ordered
              ) AS DECIMAL
       ),
       IF (
              sfo.total_refunded IS NULL,
              0,
              sfo.total_refunded
       ),
       IF (
              sfo.shipping_refunded IS NULL,
              0,
              sfo.shipping_refunded
       ),
       IF (
              cpev_outletcat_con.value IS NOT NULL,
              cpev_outletcat_con.value,
              cpev_outletcat_sim.value
       ),
       IF (
              eaov_pt_con.value IS NOT NULL,
              eaov_pt_con.value,
              eaov_pt_sim.value
       ),
       eaov_brand.value,
       cps_supplier.sup_id,
       cps_supplier.name,
       eaov_color.value,
       IFNULL(
              eaov_size.value,
              eaov_size_child.value
       ),
       sfoi_con.nego,
       category_name,
       department_type,
       sfo.updated_at,
       sfo.created_at,
       CAST(
              NULL AS datetime
       ),
       cped_price.value,
       sfo.status,
       sfoi_con.tax_amount,
       sfoi_con.tax_percent
