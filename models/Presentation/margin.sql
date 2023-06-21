{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'Date', 'data_type': 'date' },
    cluster_by = ['date','order_id','charge_type'],
    unique_key = ['date','platform_name','order_id','amount_type','transaction_type','charge_type','sku','product_id'])}}
    

Select 
a.product_key,
a.platform_key,
a.order_key,
order_id,
c.platform_name,
date,
amount_type,
transaction_type,
charge_type,
product_id,
sku,
currency_code,
sum(amount) amount
from {{ ref('fact_finances')}} a
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key
left join (select distinct product_key, product_id, product_name, sku from {{ref('dim_product')}} ) d
on a.product_key = d.product_key
left join {{ ref('dim_orders')}} e
on a.order_key = e.order_key
group by 1,2,3,4,5,6,7,8,9,10,11,12