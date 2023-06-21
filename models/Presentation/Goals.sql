{{ config(materialized='table')}} 

with all_ as (
select date_trunc(date,month)datemonth,'All' as  Business_Unit,'All Business Units' as Type,
sum(gross_amount)Gross_Revenue,sum(discounts)Discounts,-sum(refunded_amount)Refunds,
sum(shipping_price)Shipping,sum(gross_amount-discounts+refunded_amount+shipping_price)Net_Revenue,
sum(spend)Total_Ad_Spend
 from {{ ref('sales_overview') }}
where date >= '2023-01-01' group by 1,2
),
DTC as (
select date_trunc(date,month)datemonth,Business_Unit,'All DTC Customers' as Type,
sum(gross_amount)Gross_Revenue,sum(discounts)Discounts,-sum(refunded_amount)Refunds,
sum(shipping_price)Shipping,sum(gross_amount-discounts+refunded_amount+shipping_price)Net_Revenue,
cast(count(distinct customer_key) as float64) Customer_Count,
cast(count(distinct order_key) as float64) Orders,
sum(gross_amount-discounts+refunded_amount+shipping_price)/IF(count(distinct order_key)=0,1,count(distinct order_key)) as Net_AOV
 from {{ ref('sales_overview') }}
where date >= '2023-01-01' AND Business_Unit in ('DTC') group by 1,2
),
NEW_DTC AS (
    select date_trunc(date,month)datemonth,Business_Unit,'New DTC Customers' as Type,
cast(count(distinct case when CustomerType = 'New' then customer_key end) as float64)Customer_Count,
cast(count(distinct case when CustomerType = 'New' then order_key end) as float64) Orders,
sum(case when CustomerType = 'New' then gross_amount-discounts+refunded_amount+shipping_price else 0 end )Net_Revenue,
sum(case when CustomerType = 'New' then gross_amount-discounts+refunded_amount+shipping_price else 0 end)/
IF(count(distinct case when CustomerType = 'New' then order_key end)=0,1,count(distinct case when CustomerType = 'New' then order_key end)) as Net_AOV
 from {{ ref('sales_overview') }}
where date >= '2023-01-01' AND Business_Unit in ('DTC') group by 1,2
),
return_dtc as (
        select date_trunc(date,month)datemonth,Business_Unit,'Repeat DTC Customers' as Type,
        cast(count(distinct customer_key) - count(distinct case when CustomerType = 'New' then customer_key end) as float64)Customer_Count,
        cast(count(distinct case when CustomerType <> 'New' then order_key end) as float64)Orders,
        sum(case when CustomerType <> 'New' then gross_amount-discounts+refunded_amount+shipping_price else 0 end )Net_Revenue,
        sum(case when CustomerType <> 'New' then gross_amount-discounts+refunded_amount+shipping_price else 0 end)/
        IF(count(distinct case when CustomerType <> 'New' then order_key end)=0,1,
        count(distinct case when CustomerType <> 'New' then order_key end)) as Net_AOV
        from {{ ref('sales_overview') }}
where date >= '2023-01-01' AND Business_Unit in ('DTC') group by 1,2

),
cust_count as ( 
    (select date_trunc(date,year)datemonth,Business_Unit,
    'All DTC Customers' as Type,cast(count(distinct customer_key) as float64) as customer_count
    from {{ ref('sales_overview') }} 
    where date >= '2023-01-01' AND Business_Unit in ('DTC') group by 1,2)
    union all
     (select date_trunc(date,year)datemonth,Business_Unit,
    'New DTC Customers' as Type,cast(count(distinct case when CustomerType = 'New' then customer_key end) as float64) as customer_count
    from {{ ref('sales_overview') }} 
    where date >= '2023-01-01' AND Business_Unit in ('DTC') group by 1,2)
    union all
    (select date_trunc(date,year)datemonth,Business_Unit,
    'Repeat DTC Customers' as Type,cast(count(distinct customer_key) as float64)-cast(count(distinct case when CustomerType = 'New' then customer_key end) as float64) as customer_count
    from {{ ref('sales_overview') }} 
    where date >= '2023-01-01' AND Business_Unit in ('DTC') group by 1,2)
    ),
 others as (
select date_trunc(date,month)datemonth,Business_Unit,'Revenue' as Type,
sum(gross_amount)Gross_Revenue,sum(discounts)Discounts,-sum(refunded_amount)Refunds,
sum(shipping_price)Shipping,sum(gross_amount-discounts+refunded_amount+shipping_price)Net_Revenue,
sum(gross_amount-discounts+refunded_amount+shipping_price)/IF(count(distinct order_key)=0,1,count(distinct order_key)) as Net_AOV
 from {{ ref('sales_overview') }}
where date >= '2023-01-01' AND Business_Unit in ('IDP','Retail','wholesale') group by 1,2
),

final1 as (
select * from others
unpivot(VALUE for KPI in (Gross_Revenue,Discounts,Refunds,Shipping,Net_Revenue,Net_AOV) )
union all 
select * from all_
unpivot(VALUE for KPI in (Gross_Revenue,Discounts,Refunds,Shipping,Net_Revenue,Total_Ad_Spend) )
union all 
select * from dtc
unpivot(VALUE for KPI in (Gross_Revenue,Discounts,Refunds,Shipping,Net_Revenue,Customer_Count,Orders,Net_AOV) )
union all 
select * from NEW_DTC
unpivot(VALUE for KPI in (Customer_Count,Orders,Net_Revenue,Net_AOV) )
union all 
select * from return_dtc
unpivot(VALUE for KPI in (Customer_Count,Orders,Net_Revenue,Net_AOV) )
),
final2 as (select *,'M' as Period from final1
union all
select DATE('2023-01-01') as datemonth,
Business_Unit, Type,sum(VALUE)VALUE,kpi,'Y' as Period
 from final1 where kpi<>'Customer_Count'
group by 1,2,3,5),
final_cust_count as
(
    select * from cust_count 
    unpivot (VALUE for kpi in (Customer_Count))
)
select * from final2
union all 
select *, 'Y' as Period from final_cust_count