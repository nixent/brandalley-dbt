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
              WHEN sfoi_con.dispatch_date < CAST(
                     '2014-06-11' AS DATE
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
       sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) AS flash_price_exc_vat,
       sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) * sfoi_sim.qty_ordered AS line_flash_price_exc_vat,
       sfoi_con.discount_amount AS line_discount_amount,
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) /nullif(((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))),0) AS DECIMAL
       ) AS margin_inc_discount_percentage,
       CAST(
              
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) /nullif((1 +(sfoi_con.tax_percent / 100.)),0) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) AS DECIMAL
       ) AS margin_inc_discount_value,
       CAST(
              (
                     (
                            ((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)
                     ) -(
                            sfoi_sim.base_cost * sfoi_sim.qty_ordered
                     )
              ) /nullif(((sfoi_con.original_price * sfoi_sim.qty_ordered) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)),0) AS DECIMAL
       ) AS margin_exc_discount_percentage,
       CAST(
              (
                     ((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)
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
       REPLACE(REPLACE(REPLACE(cpev_gender.value, '13', 'Female'), '14', 'Male'),'11636','Unisex') AS gender,
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
                            LOWER(
                                   IF (
                                          eaov_pt_con.value IS NOT NULL,
                                          eaov_pt_con.value,
                                          eaov_pt_sim.value
                                   )
                            )
                            IN
                            ('handbags','purses','belts','umbrellas','hats','face coverings','fashion hair accessories','gloves','scarves','jewellery','tech accessories','sunglasses','travel','watches','cosmetics cases','bags','cufflinks & tie clips','ties & pocket squares','wallets','tech','accessories','ski accessories'),
                     'ACCESSORIES',
                     IF (
                                    LOWER(
                                          IF (
                                                 eaov_pt_con.value IS NOT NULL,
                                                 eaov_pt_con.value,
                                                 eaov_pt_sim.value
                                          )
                                    )
                                   in ('moisturisers','cleansers','anti-ageing','toners','masks','eye care','organic & natural','serums','gift sets','treatments','mens skincare','bath & shower','hand & foot care','bronzers & sun care','body sculpting & toning','mens bath & body','toothbrushes','teeth whitening','face','eyes','lips','makeup sets','nails','makeup removers','womens perfume','mens cologne','shampoo & conditioners','hair oils & treatments','hair styling & finishing','hair accessories','personal care electricals','mens hair care'),                            
                            'BEAUTY',
                            IF (
                                          LOWER(
                                                 IF (
                                                        eaov_pt_con.value IS NOT NULL,
                                                        eaov_pt_con.value,
                                                        eaov_pt_sim.value
                                                 )
                                          )
                                          in ('ankle boots','long boots','flat shoes','heeled shoes','court shoes','espadrilles','pumps','flat sandals','flip flops','heeled sandals','slippers','trainers','boots','casual shoes','formal shoes','sandals','footwear'),
                                   
                                   'FOOTWEAR',
                                   IF (
                                                 LOWER(
                                                        IF (
                                                               eaov_pt_con.value IS NOT NULL,
                                                               eaov_pt_con.value,
                                                               eaov_pt_sim.value
                                                        )
                                                 ) in
                                                 ('duvets & pillows','towels','bathroom accessories','bathroom fixtures','bed accessories','bed linen','mattress toppers & protectors','bath mats','beach towels','beds','mattresses','bookcases shelving units & shelves','sofas & armchairs','coffee tables','side tables','console tables','media cabinets','cabinets & sideboards','bar chairs & stools','cots & beds','seats tables & loungers','barbeque accessories','garden tools','outdoor garden lighting','planters pots & ornaments','lighting','candles & home fragrance','curtains & blinds','cushions & throws','rugs','wall art','mirrors','decorative accessories','stationery','storage','pet care','pots & pans','food preparation','utensils','cooking & baking','kitchen fixtures','kitchen storage','tableware','drinkware','cutlery','barware & drinks accessories','electricals','laundry & ironing','food & drink','decorations','nutrition supplements','sports equipment','luggage & suitcases','backpacks & holdalls','travel accessories','alcohol','chocolate & sweets','gift hampers','games & puzzles','baby & toddler toys','childrens toys','clothing & accessories','gift sets','home gifts','garden gifts','trees','lights','bedding','candles & fragrance','cards & calendars','home decor','wreaths & garlands','puddings chocolates & sweets','bbqs & accessories','outdoor tableware & serveware','garden furniture sets','garden seating','garden tables','sun loungers & swing seats','parasols & accessories','garden sheds & workshops','summerhouses & outbuildings','gazebos arbours & arches','garden storage','patio heaters fire pits & chimineas','outdoor wall & security lights','dining tables & chairs','portable & party lights','solar lights','decorative garden accessories','outdoor cushions','pots & planters','outdoor lanterns','gardening tools','outdoor toys & games','drink'),
                                          'HOME',
                                          IF (
                                                        LOWER(
                                                               IF (
                                                                      eaov_pt_con.value IS NOT NULL,
                                                                      eaov_pt_con.value,
                                                                      eaov_pt_sim.value
                                                               )
                                                        ) in
                                                        ('baby','boys clothing','girls clothing','baby shoes','boys shoes','girls shoes','games & puzzles','baby & toddler toys','childrens toys','childrens books','baby gifts','buggies & travel','nursery accessories'),
                                                 'KIDS',
                                                 IF (
                                                               LOWER(
                                                                      IF (
                                                                             eaov_pt_con.value IS NOT NULL,
                                                                             eaov_pt_con.value,
                                                                             eaov_pt_sim.value
                                                                      )
                                                               ) in
                                                               ('bras','briefs','bodies','slips','nightwear','shapewear','suspenders','socks & tights','swimwear & beachwear'),
                                                        'LINGERIE',
                                                        IF (
                                                                      LOWER(
                                                                             IF (
                                                                                    eaov_pt_con.value IS NOT NULL,
                                                                                    eaov_pt_con.value,
                                                                                    eaov_pt_sim.value
                                                                             )
                                                                      ) in
                                                                      ('activewear','blouses & tops','coats','jeans','dresses','jackets','jumpsuits','knitwear','leather','loungewear & onesies','maternity','shorts','skirts','sweatshirts & fleeces','shirts','polo shirts','suits','swimwear','trousers','t-shirts & vests','nightwear','shorts','underwear & socks','sweatshirts & hoodies','team merchandise','t-shirts','track pants','vests','ski jackets','ski trousers','sports bras','outerwear','leggings','base layers'),
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
       cast(sfo.created_at as timestamp) AS created_at,
       CAST(
              NULL AS datetime
       ) month_created,
              sfo.status AS order_status,
       sfoi_con.tax_amount,
       sfoi_con.tax_percent,
       cped_price.value AS rrp,
       ce.created_at AS reg_date,
       case
       when sfoa.postcode LIKE 'AB%' THEN 'Scotland'
       when sfoa.postcode LIKE 'AL%' THEN 'East England'
       when sfoa.postcode LIKE 'BA%' THEN 'South West'
       when sfoa.postcode LIKE 'BB%' THEN 'North West'
       when sfoa.postcode LIKE 'BD%' THEN 'North West'
       when sfoa.postcode LIKE 'BH%' THEN 'South West'
       when sfoa.postcode LIKE 'BL%' THEN 'North West'
       when sfoa.postcode LIKE 'BN%' THEN 'South East'
       when sfoa.postcode LIKE 'BR%' THEN 'Greater London'
       when sfoa.postcode LIKE 'BS%' THEN 'South West'
       when sfoa.postcode LIKE 'BT%' THEN 'Northern Ireland'
       when sfoa.postcode LIKE 'CA%' THEN 'North West'
       when sfoa.postcode LIKE 'CB%' THEN 'East England'
       when sfoa.postcode LIKE 'CF%' THEN 'Wales'
       when sfoa.postcode LIKE 'CH%' THEN 'North West'
       when sfoa.postcode LIKE 'CM%' THEN 'East England'
       when sfoa.postcode LIKE 'CO%' THEN 'East England'
       when sfoa.postcode LIKE 'CR%' THEN 'Greater London'
       when sfoa.postcode LIKE 'CT%' THEN 'South East'
       when sfoa.postcode LIKE 'CV%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'CW%' THEN 'North West'
       when sfoa.postcode LIKE 'DA%' THEN 'Greater London'
       when sfoa.postcode LIKE 'DD%' THEN 'Scotland'
       when sfoa.postcode LIKE 'DE%' THEN 'East Midlands'
       when sfoa.postcode LIKE 'DG%' THEN 'Scotland'
       when sfoa.postcode LIKE 'DH%' THEN 'North East'
       when sfoa.postcode LIKE 'DL%' THEN 'North East'
       when sfoa.postcode LIKE 'DN%' THEN 'Yorkshire & Humberside'
       when sfoa.postcode LIKE 'DT%' THEN 'South West'
       when sfoa.postcode LIKE 'DY%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'EC%' THEN 'Greater London'
       when sfoa.postcode LIKE 'EH%' THEN 'Scotland'
       when sfoa.postcode LIKE 'EN%' THEN 'Greater London'
       when sfoa.postcode LIKE 'EX%' THEN 'South West'
       when sfoa.postcode LIKE 'FK%' THEN 'Scotland'
       when sfoa.postcode LIKE 'FY%' THEN 'North West'
       when sfoa.postcode LIKE 'GL%' THEN 'South West'
       when sfoa.postcode LIKE 'GU%' THEN 'South East'
       when sfoa.postcode LIKE 'GY%' THEN 'Channel Islands'
       when sfoa.postcode LIKE 'HA%' THEN 'Greater London'
       when sfoa.postcode LIKE 'HD%' THEN 'North West'
       when sfoa.postcode LIKE 'HG%' THEN 'Yorkshire & Humberside'
       when sfoa.postcode LIKE 'HP%' THEN 'South East'
       when sfoa.postcode LIKE 'HR%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'HS%' THEN 'Scotland'
       when sfoa.postcode LIKE 'HU%' THEN 'Yorkshire & Humberside'
       when sfoa.postcode LIKE 'HX%' THEN 'North West'
       when sfoa.postcode LIKE 'IG%' THEN 'Greater London'
       when sfoa.postcode LIKE 'IM%' THEN 'North West'
       when sfoa.postcode LIKE 'IP%' THEN 'East England'
       when sfoa.postcode LIKE 'IV%' THEN 'Scotland'
       when sfoa.postcode LIKE 'JE%' THEN 'Channel Islands'
       when sfoa.postcode LIKE 'KA%' THEN 'Scotland'
       when sfoa.postcode LIKE 'KT%' THEN 'Greater London'
       when sfoa.postcode LIKE 'KW%' THEN 'Scotland'
       when sfoa.postcode LIKE 'KY%' THEN 'Scotland'
       when sfoa.postcode LIKE 'LA%' THEN 'North West'
       when sfoa.postcode LIKE 'LD%' THEN 'Wales'
       when sfoa.postcode LIKE 'LE%' THEN 'East Midlands'
       when sfoa.postcode LIKE 'LL%' THEN 'Wales'
       when sfoa.postcode LIKE 'LN%' THEN 'East England'
       when sfoa.postcode LIKE 'LS%' THEN 'Yorkshire & Humberside'
       when sfoa.postcode LIKE 'LU%' THEN 'East England'
       when sfoa.postcode LIKE 'ME%' THEN 'South East'
       when sfoa.postcode LIKE 'MK%' THEN 'South East'
       when sfoa.postcode LIKE 'ML%' THEN 'Scotland'
       when sfoa.postcode LIKE 'NE%' THEN 'North East'
       when sfoa.postcode LIKE 'NG%' THEN 'East Midlands'
       when sfoa.postcode LIKE 'NN%' THEN 'East Midlands'
       when sfoa.postcode LIKE 'NP%' THEN 'Wales'
       when sfoa.postcode LIKE 'NR%' THEN 'East England'
       when sfoa.postcode LIKE 'NW%' THEN 'Greater London'
       when sfoa.postcode LIKE 'OL%' THEN 'North West'
       when sfoa.postcode LIKE 'OX%' THEN 'South East'
       when sfoa.postcode LIKE 'PA%' THEN 'Scotland'
       when sfoa.postcode LIKE 'PE%' THEN 'East England'
       when sfoa.postcode LIKE 'PH%' THEN 'Scotland'
       when sfoa.postcode LIKE 'PL%' THEN 'South West'
       when sfoa.postcode LIKE 'PO%' THEN 'South East'
       when sfoa.postcode LIKE 'PR%' THEN 'North West'
       when sfoa.postcode LIKE 'RG%' THEN 'South East'
       when sfoa.postcode LIKE 'RH%' THEN 'South East'
       when sfoa.postcode LIKE 'RM%' THEN 'Greater London'
       when sfoa.postcode LIKE 'SA%' THEN 'Wales'
       when sfoa.postcode LIKE 'SE%' THEN 'Greater London'
       when sfoa.postcode LIKE 'SG%' THEN 'South East'
       when sfoa.postcode LIKE 'SK%' THEN 'North West'
       when sfoa.postcode LIKE 'SL%' THEN 'South East'
       when sfoa.postcode LIKE 'SM%' THEN 'Greater London'
       when sfoa.postcode LIKE 'SN%' THEN 'South West'
       when sfoa.postcode LIKE 'SO%' THEN 'South East'
       when sfoa.postcode LIKE 'SP%' THEN 'South West'
       when sfoa.postcode LIKE 'SR%' THEN 'North East'
       when sfoa.postcode LIKE 'SS%' THEN 'East England'
       when sfoa.postcode LIKE 'ST%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'SW%' THEN 'Greater London'
       when sfoa.postcode LIKE 'SY%' THEN 'Wales'
       when sfoa.postcode LIKE 'TA%' THEN 'South West'
       when sfoa.postcode LIKE 'TD%' THEN 'Scotland'
       when sfoa.postcode LIKE 'TF%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'TN%' THEN 'South East'
       when sfoa.postcode LIKE 'TQ%' THEN 'South West'
       when sfoa.postcode LIKE 'TR%' THEN 'South West'
       when sfoa.postcode LIKE 'TS%' THEN 'North East'
       when sfoa.postcode LIKE 'TW%' THEN 'Greater London'
       when sfoa.postcode LIKE 'UB%' THEN 'Greater London'
       when sfoa.postcode LIKE 'WA%' THEN 'North West'
       when sfoa.postcode LIKE 'WC%' THEN 'Greater London'
       when sfoa.postcode LIKE 'WD%' THEN 'Greater London'
       when sfoa.postcode LIKE 'WF%' THEN 'Yorkshire & Humberside'
       when sfoa.postcode LIKE 'WN%' THEN 'North West'
       when sfoa.postcode LIKE 'WR%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'WS%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'WV%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'YO%' THEN 'Yorkshire & Humberside'
       when sfoa.postcode LIKE 'ZE%' THEN 'Scotland'
       when sfoa.postcode LIKE 'Z%' THEN 'Others'
       when sfoa.postcode LIKE 'B%' THEN 'West Midlands'
       when sfoa.postcode LIKE 'W%' THEN 'Greater London'
       when sfoa.postcode LIKE 'S%' THEN 'Yorkshire & Humberside'
       when sfoa.postcode LIKE 'N%' THEN 'Greater London'
       when sfoa.postcode LIKE 'L%' THEN 'North West'
       when sfoa.postcode LIKE 'G%' THEN 'Scotland'
       when sfoa.postcode LIKE 'E%' THEN 'Greater London'
       when sfoa.postcode LIKE 'M%' THEN 'North West'
       ELSE 'Ireland'
       END as Region, -- Cat 1
       sfo.customer_email, -- Cat 1
       sfoa.address_type, -- Cat 1  
       CAST(cpn.date_comp_exported as timestamp) as date_comp_exported,
       sfoi_sim.created_at > cpn.date_comp_exported as cpn_date_flag,
       sfoi_sim.qty_backordered,
       cpn.sap_ref,
       cpn.status as cpn_status,
       MAX(
              cpe.sku
       ) AS parent_sku,
       MAX(
              cpr.reference
       ) AS REFERENCE,
       sum((sfoi_sim.qty_invoiced * sfoi_con.base_price_incl_tax) - sfoi_con.discount_amount) as TOTAL_GBP_after_vouchers -- Cat 1
FROM
       {{ ref(
           'stg__sales_flat_order'
       ) }}
       sfo
       left join {{ ref(
           'stg__sales_flat_order_address'
       ) }} sfoa on sfoa.entity_id = sfo.billing_address_id
       left join {{ ref(
           'stg__sales_flat_order_address'
       ) }} sfoa_shipping on sfoa.entity_id = sfo.shipping_address_id
        LEFT JOIN    {{ ref(
           'stg__customer_entity'
       ) }} ce ON ce.entity_id = sfo.customer_id
       LEFT JOIN {{ ref(
           'stg__sales_flat_order_item'
       ) }}
       sfoi_sim
       ON sfoi_sim.order_id = sfo.entity_id
       AND sfoi_sim.product_type = 'simple'
       LEFT JOIN {{ ref(
           'stg__sales_flat_order_item'
       ) }}
       sfoi_con
       ON sfoi_con.order_id = sfo.entity_id
       AND IF (
              sfoi_sim.parent_item_id IS NOT NULL,
              sfoi_con.item_id = sfoi_sim.parent_item_id,
              sfoi_con.item_id = sfoi_sim.item_id
       )
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_varchar'
       ) }}
       cpev_outletcat_con
       ON cpev_outletcat_con.entity_id = sfoi_con.product_id
       AND cpev_outletcat_con.attribute_id = 205
       AND cpev_outletcat_con.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_varchar'
       ) }}
       cpev_outletcat_sim
       ON cpev_outletcat_sim.entity_id = sfoi_sim.product_id
       AND cpev_outletcat_sim.attribute_id = 205
       AND cpev_outletcat_sim.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_varchar'
       ) }}
       cpev_pt_sim
       ON cpev_pt_sim.entity_id = sfoi_sim.product_id
       AND cpev_pt_sim.attribute_id = 179
       AND cpev_pt_sim.store_id = 0
       LEFT JOIN {{ ref(
           'stg__eav_attribute_option_value'
       ) }}
       eaov_pt_sim
       ON cpev_pt_sim.value = CAST(
              eaov_pt_sim.option_id AS STRING
       )
       AND eaov_pt_sim.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_varchar'
       ) }}
       cpev_pt_con
       ON cpev_pt_con.entity_id = sfoi_con.product_id
       AND cpev_pt_con.attribute_id = 179
       AND cpev_pt_con.store_id = 0
       LEFT JOIN {{ ref(
           'stg__eav_attribute_option_value'
       ) }}
       eaov_pt_con
       ON cpev_pt_con.value = CAST(
              eaov_pt_con.option_id AS STRING
       )
       AND eaov_pt_con.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_int'
       ) }}
       cpei_brand
       ON cpei_brand.entity_id = sfoi_con.product_id
       AND cpei_brand.attribute_id = 178
       AND cpei_brand.store_id = 0
       LEFT JOIN {{ ref(
           'stg__eav_attribute_option_value'
       ) }}
       eaov_brand
       ON eaov_brand.option_id = cpei_brand.value
       AND eaov_brand.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_int'
       ) }}
       cpei_color
       ON cpei_color.entity_id = sfoi_con.product_id
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
       cpei_size
       ON cpei_size.entity_id = sfoi_con.product_id
       AND cpei_size.attribute_id = 177
       AND cpei_size.store_id = 0
       LEFT JOIN {{ ref(
           'stg__eav_attribute_option_value'
       ) }}
       eaov_size
       ON eaov_size.option_id = cpei_size.value
       AND eaov_size.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_int'
       ) }}
       cpei_size_child
       ON cpei_size_child.entity_id = sfoi_sim.product_id
       AND cpei_size_child.attribute_id = 177
       AND cpei_size_child.store_id = 0
       LEFT JOIN {{ ref(
           'stg__eav_attribute_option_value'
       ) }}
       eaov_size_child
       ON eaov_size_child.option_id = cpei_size_child.value
       AND eaov_size_child.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity_int'
       ) }}
       cpei_supplier
       ON cpei_supplier.entity_id = sfoi_sim.product_id
       AND cpei_supplier.attribute_id = 239
       AND cpei_supplier.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_supplier'
       ) }}
       cps_supplier
       ON cpei_supplier.value = cps_supplier.supplier_id
       LEFT JOIN {{ ref(
           'stg__sales_flat_order_item_extra'
       ) }}
       sfoie
       ON sfoi_con.item_id = sfoie.order_item_id
       LEFT JOIN {{ ref(
           'stg__catalog_category_entity_history'
       ) }}
       cceh
       ON sfoie.category_id = cceh.category_id
       LEFT JOIN {{ ref(
           'stg__catalog_product_negotiation'
       ) }}
       cpn
       ON sfoi_sim.nego = cpn.negotiation_id
       LEFT JOIN {{ ref(
           'stg__admin_user'
       ) }}
       au
       ON cpn.buyer = au.user_id
       LEFT JOIN {{ ref(
           'stg__catalog_product_super_link'
       ) }}
       cpsl
       ON sfoi_sim.product_id = cpsl.product_id
       LEFT OUTER JOIN {{ ref(
           'stg__catalog_product_entity'
       ) }}
       cpe
       ON cpe.entity_id = cpsl.parent_id
       LEFT JOIN
        {{ ref(
           'stg__catalog_product_entity_varchar'
       ) }} cpev_gender ON cpe.entity_id = cpev_gender.entity_id
        AND cpev_gender.attribute_id = 180
        AND cpev_gender.store_id = 0
      LEFT JOIN {{ ref(
           'stg__catalog_product_entity_decimal'
       ) }}
       cped_price
       ON sfoi_sim.product_id = cped_price.entity_id
       AND cped_price.attribute_id = 75
       AND cped_price.store_id = 0
       LEFT JOIN {{ ref(
           'stg__catalog_product_entity'
       ) }}
       cpe_ref
       ON sfoi_sim.sku = cpe_ref.sku
       LEFT JOIN {{ ref(
           'stg__catalog_product_reference'
       ) }}
       cpr
       ON cpe_ref.entity_id = cpr.entity_id
WHERE
       sfo.increment_id NOT LIKE '%-%'
       AND (
              sfo.sales_product_type != 12
              OR sfo.sales_product_type IS NULL
       )
{{dbt_utils.group_by(59)}}