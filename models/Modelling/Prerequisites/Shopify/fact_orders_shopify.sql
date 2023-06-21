select
  a.order_id,
  'Shopify' as platform_name,
  currency,
  exchange_currency_code,
  exchange_currency_rate,
  date(created_at) as date,
  'Order' as transaction_type, 
  false as is_cancelled,
  sum(quantity) quantity,sum(d.line_items)line_items,
  sum(CAST(item_price as numeric)) gross_amount,
  sum(CAST(total_tax AS numeric)) total_tax, 
  sum(case when discount_type = 'shipping' then 0 
           else shipping_disc_price end) as shipping_price, 
  sum(case when discount_type = 'shipping' then 0 
           when cast(discount_amount as numeric) is null and cast(total_discounts as numeric)  = 10 then 0 
           else ifnull( cast(discount_amount as numeric),cast(total_discounts as numeric) ) end ) as discounts,
  email
  from ( select * from {{ ref('spy_orders') }} )a
  left join (
      select order_id,sum(cast(quantity as int)) quantity,
      count(distinct sku)line_items, sum(price*quantity) item_price 
      from {{ref('spy_orders_line_items')}} group by 1
      ) d
  on a.order_id=d.order_id 
  left join (select order_id,sum(cast(price as int)) shipping_price,
             sum(cast(discounted_price as int) ) shipping_disc_price from {{ref('spy_orders_shipping_lines')}} group by 1) e
  on a.order_id=e.order_id
  group by 1,2,3,4,5,6,7,8,15

  UNION ALL

  select 
  cast(a.refunds_order_id as string) as order_id,
  'Shopify' as platform_name,
  a.currency,
  a.exchange_currency_code,
  a.exchange_currency_rate,
  date(a.refunds_created_at) as date,
  'Return' as transaction_type, 
  false as is_cancelled,
  -sum(return_quantity) as quantity,-sum(e.line_items) as line_items,
  -sum(case when transactions_amount is null then cast(subtotal as numeric) 
  else  cast(transactions_amount as numeric ) - coalesce(refund_total_tax,0) end ) as gross_amount,
  -sum(refund_total_tax) total_tax, 
  0 as shipping_price,
  -sum(total_discount) as discounts,
  email
  from {{ ref('spy_refunds_transactions')}} a 
  left join (select cast(refunds_order_id as string) as order_id, sum(cast(refund_line_items_quantity as int)) return_quantity, 
             sum(subtotal) subtotal, sum(refund_line_items_total_tax) refund_total_tax 
             from {{ref('spy_orders_refund_lines')}} group by 1) d
  on cast(a.refunds_order_id as string)=d.order_id

  left join (select cast(refunds_order_id as string) as order_id,count(distinct sku)line_items,
             sum(cast(total_discount as numeric)) total_discount, sum(price*quantity) item_price 
             from {{ref('spy_orders_refunds_line_items')}} group by 1) e
  on cast(a.refunds_order_id as string)=e.order_id

  left join ( select distinct cast(order_id as string) as order_id, discount_code,tags from {{ref('spy_orders')}} ) f
  on cast(a.refunds_order_id as string)=f.order_id

  where refunds_created_at is not null and transactions_status = 'success' 
  group by 1,2,3,4,5,6,7,8,15
