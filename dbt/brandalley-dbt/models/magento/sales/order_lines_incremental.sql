{{ config(
	materialized='incremental',
	unique_key='unique_id'
) }}

select
	sha1(concat(
		sfoi_con.product_id,
		sfoi_con.order_id,
		sfoi_con.item_id,
		sfo.entity_id,
		ifnull (cast(cpev_pt_con.value as string), '_'),
		ifnull (cast(eaov_brand.option_id as string), '_'),
		ifnull (cast(eaov_color.option_id as string), '_'),
		ifnull (cast(eaov_size.option_id as string), '_'),
		ifnull (cast(cpei_size_child.entity_id as string), '_'),
		ifnull (cast(eaov_size_child.option_id as string), '_')
	)) as unique_id,
	datetime_diff(safe_cast(safe_cast(sfo.created_at as timestamp) as datetime), safe_cast(safe_cast(ce.created_at as timestamp) as datetime), month) as months_since_cohort_start,
	datetime_diff(safe_cast(safe_cast(sfo.created_at as timestamp) as datetime), safe_cast(safe_cast(ce.created_at as timestamp) as datetime), year) as years_since_cohort_start,
	datetime_diff(safe_cast(safe_cast(sfo.created_at as timestamp) as datetime), safe_cast(safe_cast(ce.created_at as timestamp) as datetime), quarter) as quarters_since_cohort_start,
	sfo.increment_id as order_number,
	sfo.customer_id as customer_id,
	sfoi_sim.item_id as order_item_id,
	sfo.entity_id as order_id,
	sfoi_sim.sku,
	if(au.user_id is not null, concat(au.firstname, ' ', au.lastname), 'Unknown') as buyer,
	sfoi_con.name,
	sfoi_con.qty_canceled,
	sfoi_sim.qty_ordered,
	sfoi_sim.qty_invoiced,
	sfoi_con.qty_refunded,
	sfoi_con.qty_shipped,
	if(sfoi_sim.qty_backordered is null or cpn.type=30, 0, sfoi_sim.qty_backordered) as consignment_qty,
	if(sfoi_sim.qty_backordered is null or cpn.type!=30, 0, sfoi_sim.qty_backordered) as selffulfill_qty,
	if(sfoi_sim.qty_backordered is null, sfoi_sim.qty_ordered, sfoi_sim.qty_ordered - sfoi_sim.qty_backordered) as warehouse_qty,
	safe_cast(safe_cast(sfo.created_at as timestamp) as datetime) as order_placed_date,
    case
		when sfoi_con.dispatch_date < cast('2014-06-11' as date) then null
		else sfoi_con.dispatch_date
	end as dispatch_due_date,
	sfoi_sim.base_cost as product_cost_inc_vat,
	(sfoi_sim.base_cost * sfoi_sim.qty_ordered) as line_product_cost_inc_vat,
	cast((sfoi_sim.base_cost) as decimal) as product_cost_exc_vat,
	(sfoi_sim.base_cost) * sfoi_sim.qty_ordered as line_product_cost_exc_vat,
	sfoi_con.original_price as flash_price_inc_vat,
	sfoi_con.original_price * sfoi_sim.qty_ordered as line_flash_price_inc_vat,
	sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) as flash_price_exc_vat,
	sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0) * sfoi_sim.qty_ordered as line_flash_price_exc_vat,
	sfoi_con.discount_amount as line_discount_amount,
	cast(
			(
					((sfoi_con.original_price * sfoi_sim.qty_ordered) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)) -(
						sfoi_sim.base_cost * sfoi_sim.qty_ordered
					)
			) /nullif(((sfoi_con.original_price * sfoi_sim.qty_ordered) /(1 +(sfoi_con.tax_percent / 100.))),0) as decimal
	) as margin_inc_discount_percentage,
	cast(
			
					((sfoi_con.original_price * sfoi_sim.qty_ordered) /nullif((1 +(sfoi_con.tax_percent / 100.)),0) -(
						sfoi_sim.base_cost * sfoi_sim.qty_ordered
					)
			) as decimal
	) as margin_inc_discount_value,
	cast(
			(
					(
						((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)
					) -(
						sfoi_sim.base_cost * sfoi_sim.qty_ordered
					)
			) /nullif(((sfoi_con.original_price * sfoi_sim.qty_ordered) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)),0) as decimal
	) as margin_exc_discount_percentage,
	cast(
			(
					((sfoi_con.original_price * sfoi_sim.qty_ordered) - sfoi_con.discount_amount) /nullif((1 +(sfoi_con.tax_percent / 100.)),0)
			) -(
					sfoi_sim.base_cost * sfoi_sim.qty_ordered
			) as decimal
	) as margin_exc_discount_value,
	if(sfo.total_refunded is null, 0, sfo.total_refunded) as line_total_refunded,
	if(sfo.shipping_refunded is null, 0, sfo.shipping_refunded) as shipping_refunded,
	if(cpev_outletcat_con.value is not null, cpev_outletcat_con.value, cpev_outletcat_sim.value) as category_path,
	if(eaov_pt_con.value is not null, eaov_pt_con.value, eaov_pt_sim.value) as product_type,
	eaov_brand.value as brand,
	cps_supplier.sup_id as supplier_id,
	cps_supplier.name as supplier_name,
	eaov_color.value as colour,
	replace(replace(replace(cpev_gender.value, '13', 'Female'), '14', 'Male'),'11636','Unisex') as gender,
	ifnull(eaov_size.value, eaov_size_child.value) as SIZE,
	sfoi_con.nego,
	case
		when cceh.name in ('', 'Women', 'Men', 'Kids', 'Lingerie', 'Home', 'Beauty', 'Z_NoData', 'Archieved outlet products', 'Holding review')
			or cceh.name is null
	 	then 'Outlet'
		else cceh.name
	end as category_name,
	case cceh.type
		when 1 then if (
			LOWER(
					IF (
							eaov_pt_con.value IS NOT NULL,
							eaov_pt_con.value,
							eaov_pt_sim.value
					)
			)
			IN
			('handbags','purses','belts','umbrellas','hats','face coverings','fashion hair accessories','gloves','scarves','jewellery','tech accessories','sunglasses','travel','watches','cosmetics cases','bags','cufflinks & tie clips','ties & pocket squares','wallets','tech','accessories','ski accessories','luggage & suitcases','backpacks & holdalls','travel accessories'),
		'ACCESSORIES',
		IF (
					LOWER(
							IF (
									eaov_pt_con.value IS NOT NULL,
									eaov_pt_con.value,
									eaov_pt_sim.value
							)
					)
					in ('moisturisers','cleansers','anti-ageing','toners','masks','eye care','organic & natural','serums','gift sets','treatments','mens skincare','bath & shower','hand & foot care','bronzers & sun care','body sculpting & toning','mens bath & body','toothbrushes','teeth whitening','face','eyes','lips','makeup sets','nails','makeup removers','womens perfume','mens cologne','shampoo & conditioners','hair oils & treatments','hair styling & finishing','hair accessories','personal care electricals','mens hair care','Skincare','Bodycare','Haircare','candles & home fragrance','candles & fragrance','gift sets'),                            
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
									('barbeque accessories','pots & pans','food preparation','utensils','cooking & baking','kitchen fixtures','kitchen storage','tableware','drinkware','cutlery','barware & drinks accessories','electricals','laundry & ironing','food & drink','nutrition supplements','alcohol','chocolate & sweets','gift hampers','puddings chocolates & sweets','bbqs & accessories','outdoor tableware & serveware','drink','Dining','Cookware/bakeware'),
							'K&D',
							IF (
										LOWER(
												IF (
														eaov_pt_con.value IS NOT NULL,
														eaov_pt_con.value,
														eaov_pt_sim.value
												)
										) in
										('garden tools','outdoor garden lighting','planters pots & ornaments','curtains & blinds','cushions & throws','rugs','wall art','decorative accessories','stationery','storage','pet care','decorations','home gifts','garden gifts','trees','lights','cards & calendars','home decor','wreaths & garlands','outdoor wall & security lights','portable & party lights','solar lights','decorative garden accessories','outdoor cushions','pots & planters','outdoor lanterns','gardening tools','Cards & Crackers','Christmas Decorations','Gifts','Interior'),
									'DEC HOME',
									IF (
												LOWER(
														IF (
																eaov_pt_con.value IS NOT NULL,
																eaov_pt_con.value,
																eaov_pt_sim.value
														)
												) in
												('beds','mattresses','bookcases shelving units & shelves','sofas & armchairs','coffee tables','side tables','console tables','media cabinets','cabinets & sideboards','bar chairs & stools','cots & beds','seats tables & loungers','lighting','mirrors','garden furniture sets','garden seating','garden tables','sun loungers & swing seats','parasols & accessories','garden sheds & workshops','summerhouses & outbuildings','gazebos arbours & arches','garden storage','patio heaters fire pits & chimineas','dining tables & chairs'),
										'FURN',
										IF (
														LOWER(
																IF (
																	eaov_pt_con.value IS NOT NULL,
																	eaov_pt_con.value,
																	eaov_pt_sim.value
																)
														) in
														('duvets & pillows','towels','bathroom accessories','bathroom fixtures','bed accessories','bed linen','mattress toppers & protectors','bath mats','beach towels','bedding','Bath Robes','Blankets','Bedroom','Bathroom'),
												'B&B',
												IF (
															LOWER(
																	IF (
																			eaov_pt_con.value IS NOT NULL,
																			eaov_pt_con.value,
																			eaov_pt_sim.value
																	)
															) in
															('baby','boys clothing','girls clothing','baby shoes','boys shoes','girls shoes','games & puzzles','baby & toddler toys','childrens toys','childrens books','baby gifts','buggies & travel','nursery accessories','Wooden Toys','Babygrows','Sleepsuits','Outdoor Play','Soft Toys','Boys','Girls Footwear','games & puzzles','baby & toddler toys','childrens toys','outdoor toys & games'),
													'KIDS',
														IF (
																LOWER(
																		IF (
																				eaov_pt_con.value IS NOT NULL,
																				eaov_pt_con.value,
																				eaov_pt_sim.value
																		)
																) in
																('bras','briefs','bodies','slips','nightwear','shapewear','suspenders','socks & tights','swimwear & beachwear','Underwear','Suspender belts','swimwear','nightwear','underwear & socks','activewear','sports bras','leggings','Sportswear/sports accessories','sports equipment'),
															'LINGERIE',
																IF (
																		LOWER(
																				IF (
																						eaov_pt_con.value IS NOT NULL,
																						eaov_pt_con.value,
																						eaov_pt_sim.value
																				)
																		) in
																		('blouses & tops','coats','jeans','dresses','jackets','jumpsuits','knitwear','leather','loungewear & onesies','maternity','shorts','skirts','sweatshirts & fleeces','shirts','polo shirts','suits','trousers','t-shirts & vests','shorts','sweatshirts & hoodies','team merchandise','t-shirts','track pants','vests','ski jackets','ski trousers','outerwear','base layers','Tops/T-Shirts','Camisoles/tops','Blouses/shirts','Loungewear'),
																	'RTW',
																	'OUTLET'
																)
														)
												)
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
       cast(sfo.created_at as timestamp) as created_at,
       cast(null as datetime) as month_created,
       sfo.status AS order_status,
       sfoi_con.tax_amount,
       sfoi_con.tax_percent,
       cped_price.value as rrp,
       ce.created_at as reg_date,
       {{ postcode_region('sfo.billing_postcode') }} as Region, -- Cat 1
       sfo.customer_email, -- Cat 1
       sfo.billing_address_type, -- Cat 1  
       CAST(cpn.date_comp_exported as timestamp) as date_comp_exported,
       sfoi_sim.created_at > cpn.date_comp_exported as cpn_date_flag,
       sfoi_sim.qty_backordered,
       cpn.sap_ref,
       cpn.status as cpn_status,
       eaov_product_age.value as product_age,
       max(cpe.sku) as parent_sku,
       max(cpr.reference) as REFERENCE,
       sum((sfoi_sim.qty_ordered * sfoi_con.original_price) - sfoi_con.discount_amount) as TOTAL_GBP_after_vouchers,
       sum(sfoi_sim.qty_ordered * sfoi_con.original_price) as TOTAL_GBP_before_vouchers,
       sum(sfoi_sim.qty_ordered * (sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0)) - sfoi_con.discount_amount) as TOTAL_GBP_ex_tax_after_vouchers,
       sum(sfoi_sim.qty_ordered * (sfoi_con.original_price /nullif((1 + (sfoi_con.tax_percent / 100.)),0))) as TOTAL_GBP_ex_tax_before_vouchers,
       sum(if (
              sfoi_sim.qty_backordered is null or cpn.type!=30,
              0,
              sfoi_sim.qty_backordered
       ) * sfoi_con.base_price_incl_tax) as selffulfill_totalGBP_inc_tax,
       sum(if (
              sfoi_sim.qty_backordered is null or cpn.type!=30,
              0,
              sfoi_sim.qty_backordered
       ) * sfoi_con.base_price) as selffulfill_totalGBP_ex_tax,       
       sum(if (
              sfoi_sim.qty_backordered is null or cpn.type=30,
              0,
              sfoi_sim.qty_backordered
       ) * sfoi_con.base_price_incl_tax) as consignment_totalGBP_inc_tax,
       sum(if (
              sfoi_sim.qty_backordered is null or cpn.type=30,
              0,
              sfoi_sim.qty_backordered
       ) * sfoi_con.base_price) as consignment_totalGBP_ex_tax,       
       sum(if (
              sfoi_sim.qty_backordered is null,
              sfoi_sim.qty_ordered,
              sfoi_sim.qty_ordered - sfoi_sim.qty_backordered
       ) * sfoi_con.base_price_incl_tax) as warehouse_totalGBP_inc_tax,
       sum(if (
              sfoi_sim.qty_backordered is null,
              sfoi_sim.qty_ordered,
              sfoi_sim.qty_ordered - sfoi_sim.qty_backordered
       ) * sfoi_con.base_price) as warehouse_totalGBP_ex_tax
from {{ ref('orders_incremental') }} sfo
-- left join {{ ref('stg__sales_flat_order_address') }} sfoa on sfoa.entity_id = sfo.billing_address_id
-- left join {{ ref('stg__sales_flat_order_address') }} sfoa_shipping on sfoa_shipping.entity_id = sfo.shipping_address_id
left join {{ ref('stg__customer_entity') }} ce 
	on ce.entity_id = sfo.customer_id
left join {{ ref('stg__sales_flat_order_item') }} sfoi_sim
	on sfoi_sim.order_id = sfo.entity_id
       	and sfoi_sim.product_type = 'simple'
left join {{ ref('stg__sales_flat_order_item') }} sfoi_con
	on sfoi_con.order_id = sfo.entity_id
       	and if (sfoi_sim.parent_item_id is not null, sfoi_con.item_id = sfoi_sim.parent_item_id, sfoi_con.item_id = sfoi_sim.item_id)
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outletcat_con
	on cpev_outletcat_con.entity_id = sfoi_con.product_id
       	and cpev_outletcat_con.attribute_id = 205
       	and cpev_outletcat_con.store_id = 0
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_outletcat_sim
	on cpev_outletcat_sim.entity_id = sfoi_sim.product_id
       	and cpev_outletcat_sim.attribute_id = 205
       	and cpev_outletcat_sim.store_id = 0
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pt_sim
	on cpev_pt_sim.entity_id = sfoi_sim.product_id
       	and cpev_pt_sim.attribute_id = 179
       	and cpev_pt_sim.store_id = 0
left join {{ ref('stg__eav_attribute_option_value') }} eaov_pt_sim
	on cpev_pt_sim.value = cast(eaov_pt_sim.option_id as string)
       	and eaov_pt_sim.store_id = 0
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_pt_con
	on cpev_pt_con.entity_id = sfoi_con.product_id
       	and cpev_pt_con.attribute_id = 179
       	and cpev_pt_con.store_id = 0
left join {{ ref('stg__eav_attribute_option_value') }}eaov_pt_con
	on cpev_pt_con.value = cast(eaov_pt_con.option_id as string)
       	and eaov_pt_con.store_id = 0
left join {{ ref('stg__catalog_product_entity_int') }} cpei_brand
	on cpei_brand.entity_id = sfoi_con.product_id
       	and cpei_brand.attribute_id = 178
       	and cpei_brand.store_id = 0
left join {{ ref('stg__eav_attribute_option_value') }} eaov_brand
	on eaov_brand.option_id = cpei_brand.value
       	and eaov_brand.store_id = 0
left join {{ ref('stg__catalog_product_entity_int') }} cpei_color
	on cpei_color.entity_id = sfoi_con.product_id
       	and cpei_color.attribute_id = 213
       	and cpei_color.store_id = 0
left join {{ ref('stg__eav_attribute_option_value') }} eaov_color
	on eaov_color.option_id = cpei_color.value
       	and eaov_color.store_id = 0
left join {{ ref('stg__catalog_product_entity_int') }} cpei_product_age
	on cpei_product_age.entity_id = sfoi_con.product_id
       	and cpei_product_age.attribute_id = 213
       	and cpei_product_age.store_id = 0
left join {{ ref('stg__eav_attribute_option_value') }} eaov_product_age
	on eaov_product_age.option_id = cpei_product_age.value
       	and eaov_product_age.store_id = 0
left join {{ ref('stg__catalog_product_entity_int') }} cpei_size
	on cpei_size.entity_id = sfoi_con.product_id
       	and cpei_size.attribute_id = 177
       	and cpei_size.store_id = 0
left join {{ ref('stg__eav_attribute_option_value') }} eaov_size
	on eaov_size.option_id = cpei_size.value
       	and eaov_size.store_id = 0
left join {{ ref('stg__catalog_product_entity_int') }} cpei_size_child
	on cpei_size_child.entity_id = sfoi_sim.product_id
       	and cpei_size_child.attribute_id = 177
       	and cpei_size_child.store_id = 0
left join {{ ref('stg__eav_attribute_option_value') }} eaov_size_child
	on eaov_size_child.option_id = cpei_size_child.value
       	and eaov_size_child.store_id = 0
left join {{ ref('stg__catalog_product_entity_int') }} cpei_supplier
	on cpei_supplier.entity_id = sfoi_sim.product_id
       	and cpei_supplier.attribute_id = 239
       	and cpei_supplier.store_id = 0
left join {{ ref('stg__catalog_product_supplier') }} cps_supplier
	on cpei_supplier.value = cps_supplier.supplier_id
left join {{ ref('stg__sales_flat_order_item_extra') }} sfoie
	on sfoi_con.item_id = sfoie.order_item_id
left join {{ ref('stg__catalog_category_entity_history') }} cceh
	on sfoie.category_id = cceh.category_id
left join {{ ref('stg__catalog_product_negotiation') }} cpn
	on sfoi_sim.nego = cpn.negotiation_id
left join {{ ref('stg__admin_user') }} au
	on cpn.buyer = au.user_id
left join {{ ref('stg__catalog_product_super_link') }} cpsl
	on sfoi_sim.product_id = cpsl.product_id
left outer join {{ ref('stg__catalog_product_entity') }} cpe
	on cpe.entity_id = cpsl.parent_id
left join {{ ref('stg__catalog_product_entity_varchar') }} cpev_gender 
	on cpe.entity_id = cpev_gender.entity_id
        and cpev_gender.attribute_id = 180
        and cpev_gender.store_id = 0
left join {{ ref('stg__catalog_product_entity_decimal') }} cped_price
	on sfoi_sim.product_id = cped_price.entity_id
       	and cped_price.attribute_id = 75
       	and cped_price.store_id = 0
left join {{ ref('stg__catalog_product_entity') }} cpe_ref
	on sfoi_sim.sku = cpe_ref.sku
left join {{ ref('stg__catalog_product_reference') }} cpr
	on cpe_ref.entity_id = cpr.entity_id
where sfo.increment_id not like '%-%'
	and (sfo.sales_product_type != 12 or sfo.sales_product_type is null)
{{dbt_utils.group_by(64)}}

