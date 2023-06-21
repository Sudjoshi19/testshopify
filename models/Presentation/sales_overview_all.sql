{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['date'],
    unique_key = ['date','platform_name','Business_Unit','CustomerType','customer_key','order_key'])}}

with orders as (
    select 
    platform_key,
    order_key,
    date,
    currency_code,
    sum(quantity) quantity,
    sum(line_items)line_items,
    sum(gross_amount) gross_amount,
    sum(shipping_price) shipping_price,
    sum(discounts) discounts,
    sum(total_tax) total_taxes
    from {{ ref('fact_orders') }}
    where transaction_type = 'Order'
    group by 1,2,3,4
),

orders1 as (
    select o.*,cmf1.fee*1 as shipping_cost,cmf2.fee*1 as packaging_costs,cmf3.fee*gross_amount as direct_labor
from orders o 
left join {{ ref('CM_Fee') }} cmf1 on o.date>=cmf1.start_date and o.date<=cmf1.end_Date and cmf1.fee_Type="Shipping Costs"
left join {{ ref('CM_Fee') }} cmf2 on o.date>=cmf2.start_date and o.date<=cmf2.end_Date and cmf2.fee_Type="Packaging Costs"
left join {{ ref('CM_Fee') }} cmf3 on o.date>=cmf3.start_date and o.date<=cmf3.end_Date and cmf3.fee_Type="Direct Labor"
),

orders2 as (select a.*,b.fees
from orders1 a 
left join (select order_key,sum(amount) as fees from {{ ref('margin') }} where amount_type='Fees' and transaction_type='Order'
and charge_type='charge' group by 1) b on a.order_key=b.order_key
),

refunds as (
    select 
    platform_key,order_key,
    date,
    sum(coalesce(line_items,0)) refunded_line_items,
    sum(coalesce(quantity,0)) refunded_quantity,
    sum(coalesce(total_tax,0)) refunded_tax,
    sum(gross_amount) refunded_amount
    from {{ ref('fact_orders') }}
    where transaction_type = 'Return'
    group by 1,2,3
),

customer as (
    select distinct order_key,customer_key,date(order_date)order_date,date(acquisition_date)acquisition_date,
    case when date(order_date) = date(acquisition_date) then 'New' else 'Existing' end as CustomerType
    from {{ ref('dim_customer') }}
)

select platform_name,Business_Unit,CustomerType,customer_key,currency_code,f.order_key,date,
quantity,line_items,
gross_amount,shipping_price,discounts,total_taxes,
refunded_quantity,refunded_line_items,refunded_amount,refunded_tax,fees,shipping_cost,packaging_costs,direct_labor from
(
    Select 
    coalesce(a.date,b.date)date,
    coalesce(a.platform_key,b.platform_key)platform_key,
    currency_code,
    coalesce(a.order_key,b.order_key)order_key,
    coalesce(quantity,0)quantity,
    coalesce(line_items,0)line_items,
    coalesce(gross_amount,0)gross_amount,
    coalesce(shipping_price,0)shipping_price,
    coalesce(discounts,0)discounts,
    coalesce(total_taxes,0)total_taxes,
    coalesce(refunded_quantity,0)refunded_quantity,
    coalesce(refunded_line_items,0)refunded_line_items,
    coalesce(refunded_amount,0)refunded_amount,
    coalesce(refunded_tax,0)refunded_tax,
    coalesce(fees,0)fees,
    coalesce(shipping_cost,0)shipping_cost,
    coalesce(packaging_costs,0)packaging_costs,
    coalesce(round(direct_labor,2),0)direct_labor

    from orders2 a
    full outer join refunds b
    on a.platform_key = b.platform_key and a.date = b.date and a.order_key = b.order_key
    ) f

left join {{ ref('dim_channel_shopify') }} b
on f.order_key = b.order_key

left join {{ ref('dim_platform') }} c
on f.platform_key = c.platform_key

left join customer d
on f.order_key = d.order_key
