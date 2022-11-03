SELECT
    CASE
        WHEN cpev.value LIKE ('Home>%') THEN "Homeware"
        WHEN cpev.value LIKE ('Home & Garden>%') THEN "Homeware"
        WHEN cpev.value LIKE ('Gifts>%') THEN "Festival"
        WHEN cpev.value LIKE ('Kids>%') THEN "Kidswear"
        WHEN cpev.value LIKE ('Christmas>%') THEN "Festival"
        WHEN cpev.value LIKE ('Lingerie>%') THEN "Lingerie & Swimwear"
        WHEN cpev.value LIKE ('Outlet>%') THEN "Outlet"
        WHEN cpev.value LIKE ('Men>Tops%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Trousers>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Underwear>%') THEN "Lingerie & Swimwear"
        WHEN cpev.value LIKE ('Men>Accessories>%') THEN "Accessories"
        WHEN cpev.value LIKE ('Men>Clothing>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Fleeces%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Jeans>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Knitwear>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Onesies>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Outerwear>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Shirts>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Footwear>%') THEN "Footwear"
        WHEN cpev.value LIKE ('Men>Shoes>%') THEN "Footwear"
        WHEN cpev.value LIKE ('Men>Shorts>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Sportswear%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Suits>%') THEN "RTW"
        WHEN cpev.value LIKE ('Men>Swimwear>%') THEN "Lingerie & Swimwear"
        WHEN cpev.value LIKE ('Men>Nightwear>%') THEN "Lingerie & Swimwear"
        WHEN cpev.value LIKE ('Sports%') THEN "Active"
        WHEN cpev.value LIKE ('Women>Accessories%') THEN "Accessories"
        WHEN cpev.value LIKE ('Women>Blouses%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Clothing>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Dresses>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Fleeces%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Footwear%') THEN "Footwear"
        WHEN cpev.value LIKE ('Women>Handbags>%') THEN "Accessories"
        WHEN cpev.value LIKE ('Women>Jeans>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Knitwear>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Lingerie>%') THEN "Lingerie & Swimwear"
        WHEN cpev.value LIKE ('Women>Onesies>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Outerwear>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Playsuits%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Shoes%') THEN "Footwear"
        WHEN cpev.value LIKE ('Women>Shorts>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Skirts>%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Sportswear%') THEN "Active"
        WHEN cpev.value LIKE ('Women>Tops%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Trousers%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Maternity%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Jeans%') THEN "RTW"
        WHEN cpev.value LIKE ('Women>Outerwear%') THEN "RTW"
        WHEN cpev.value LIKE ('Beauty>%') THEN "Beauty"
        ELSE "Others"
    END AS category,
    sfo.status,
    sfo.customer_id,
    sfo.increment_id
FROM
    {{ ref('stg__sales_flat_order') }}
    sfo
    LEFT JOIN {{ ref('stg__customer_entity') }}
    ce
    ON ce.entity_id = sfo.customer_id
    LEFT JOIN {{ ref('stg__sales_flat_order_item') }}
    sfoi
    ON sfo.entity_id = sfoi.order_id
    AND sfoi.parent_item_id IS NULL
    LEFT JOIN {{ ref('stg__catalog_product_entity_varchar') }}
    cpev
    ON cpev.entity_id = sfoi.product_id
    AND cpev.attribute_id = 205
