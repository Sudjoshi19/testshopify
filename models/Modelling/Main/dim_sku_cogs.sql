with orders as (
      select order_id,sku,date(created_at)order_date, 
      sum(cast(quantity as int)) quantity
      from {{ref('spy_orders_line_items')}}
      where order_id is not null and sku is not null
      group by 1,2,3

),
refunds as (
select cast(refunds_order_id as string) as order_id,sku,
       -sum(refund_line_items_quantity)quantity
       from {{ref('spy_orders_refunds_line_items')}} 
       where refunds_order_id is not null and sku is not null
       group by 1,2
),
cte as (
select order_id,sku,sum(quantity)quantity from 
(
    select order_id,sku,quantity from orders union all select * from refunds
) group by 1,2
),

cte2 as (
select *,count(distinct Embroidered) over(partition by order_id) e2 from (
select *,CASE WHEN SKU LIKE '%-E' and quantity > 0 THEN 'Y' ELSE 'N' end as Embroidered
from cte )
)

select a.order_id,b.order_date,a.sku,a.quantity, 
case when e2 = 2 then 'Y' else Embroidered end as Embroidered
from cte2 a left join orders b 
on a.order_id = b.order_id
