{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['date'],
    unique_key = ['date','platform_name','order_key','Business_Unit','sku'])}}

with orders as (
    select 
    platform_key,order_key,sku_key,date,
    sum(quantity) quantity,
    sum(gross_amount) gross_amount,
    from {{ ref('fact_order_lines')}} a
    where transaction_type = 'Order'
    group by 1,2,3,4
),

refunds as (
    select 
    platform_key,order_key,sku_key,date,
    sum(quantity) return_quantity,   
    sum(gross_amount) refunds
    from {{ ref('fact_order_lines') }}
    where transaction_type = 'Return'
    group by 1,2,3,4
)

select 
date,h.platform_name,f.order_key,Business_Unit,sku,
quantity,
gross_amount,
return_quantity,
refunds   
from (
select 
coalesce(a.platform_key,e.platform_key) platform_key,
coalesce(a.order_key,e.order_key) order_key,
coalesce(a.sku_key,e.sku_key) sku_key,
coalesce(a.date,e.date) date,
coalesce(quantity,0)quantity,
coalesce(gross_amount,0)gross_amount,
coalesce(return_quantity,0)return_quantity,
coalesce(refunds,0)refunds

from orders a
full outer join refunds e
on a.platform_key = e.platform_key and a.date = e.date and a.sku_key = e.sku_key and a.order_key = e.order_key
) f

left join {{ ref('dim_platform')}} h
on f.platform_key = h.platform_key

left join (select distinct sku_key, sku from {{ref('dim_product')}} ) i
on f.sku_key = i.sku_key

left join {{ ref('dim_channel_shopify')}} j
on f.order_key = j.order_key