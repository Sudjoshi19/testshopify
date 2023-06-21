
select
order_id,
'Shopify' as platform_name,
-- cast(product_id as string) as product_id, 
sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date(created_at) as date,
'Order' as transaction_type, 
false as is_cancelled,
sum(quantity) quantity,
sum(price*quantity) gross_amount,
email
from {{ ref('spy_orders_line_items') }} 
group by 1,2,3,4,5,6,7,8,9,12

UNION ALL

select 
cast(refunds_order_id as string) as order_id,
'Shopify' as platform_name,
-- cast(product_id as string) as product_id, 
replace(sku,null,'')sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date(refunds_created_at) as date,
'Return' as transaction_type, 
false as is_cancelled,
sum(quantity) as quantity,
sum(quantity*price) gross_amount,
email
from {{ ref('spy_orders_refunds_line_items')}} 
where refunds_created_at is not null
group by 1,2,3,4,5,6,7,8,9,12