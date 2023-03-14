select 
sfo.increment_id as order_id, sfo.status, sfo.base_gift_cards_amount, sfo.gift_cards_amount, sfo.base_gift_cards_invoiced, sfo.gift_cards_invoiced, sfo.base_gift_cards_refunded, 
sfo.gift_cards_refunded, timestamp(sfo.created_at) order_date, sfo.gw_id, sfo.gw_allow_gift_receipt, sfo.gw_add_card, sfo.gw_base_price, sfo.gw_price, sfo.gw_items_base_price, sfo.gw_items_price, sfo.gw_card_base_price, 
sfo.gw_card_price, sfo.gw_base_tax_amount, sfo.gw_tax_amount, sfo.gw_items_base_tax_amount, sfo.gw_items_tax_amount, sfo.gw_card_base_tax_amount, sfo.gw_card_tax_amount, 
sfo.gw_base_price_invoiced, sfo.gw_price_invoiced, sfo.gw_items_base_price_invoiced, sfo.gw_items_price_invoiced, sfo.gw_card_base_price_invoiced, sfo.gw_card_price_invoiced, 
sfo.gw_base_tax_amount_invoiced, sfo.gw_tax_amount_invoiced, sfo.gw_items_base_tax_invoiced, sfo.gw_items_tax_invoiced, sfo.gw_card_base_tax_invoiced, sfo.gw_card_tax_invoiced, 
sfo.gw_base_price_refunded, sfo.gw_price_refunded, sfo.gw_items_base_price_refunded, sfo.gw_items_price_refunded, sfo.gw_card_base_price_refunded, sfo.gw_card_price_refunded, 
sfo.gw_base_tax_amount_refunded, sfo.gw_tax_amount_refunded, sfo.gw_items_base_tax_refunded, sfo.gw_items_tax_refunded, sfo.gw_card_base_tax_refunded, sfo.gw_card_tax_refunded, 
sfo.reward_points_balance, sfo.base_reward_currency_amount, sfo.reward_currency_amount, sfo.base_rwrd_crrncy_amt_invoiced, sfo.rwrd_currency_amount_invoiced, 
sfo.base_rwrd_crrncy_amnt_refnded, sfo.rwrd_crrncy_amnt_refunded, sfo.reward_points_balance_refund, sfo.reward_points_balance_refunded, sfo.reward_salesrule_points,
gca.date_created, gca.date_expires, gca.balance, gca.state
 from {{ ref(
        'stg__enterprise_giftcardaccount'
    ) }} gca
left outer join 
{{ ref(
        'stg__sales_flat_order'
    ) }} sfo
on REGEXP_EXTRACT(sfo.gift_cards, 'BAGC[^"]*')=gca.code