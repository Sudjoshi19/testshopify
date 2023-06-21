{{ config(materialized='table')}} 
with cte as (select * from {{ ref('target_all') }} union all 
select * from {{ ref('target_dtc') }} union all 
select * from {{ ref('target_idp') }} union all 
select * from {{ ref('target_retail') }} union all 
select * from {{ ref('target_wholesale') }}
),
final as (
select FORMAT_DATE("%Y-%m-%d",PARSE_DATE("%B %Y",month))  datemonth,Month,
Business_Unit,Type,VALUE,
case when Type = 'New DTC Customers' and KPI = 'New_Customer_Net_AOV' then 'Net_AOV'
when Type = 'New DTC Customers' and KPI = 'New_Customer_Net_Revenue' then 'Net_Revenue'
when Type = 'New DTC Customers' and KPI = 'New_Customer_Count' then 'Customer_Count'
when Type = 'New DTC Customers' and KPI = 'New_Customer_Orders' then 'Orders'
when Type = 'Repeat DTC Customers' and KPI = 'Repeat_Customer_Net_Revenue' then 'Net_Revenue'
when Type = 'Repeat DTC Customers' and KPI = 'Repeat_Customer_Net_AOV' then 'Net_AOV'
when Type = 'Repeat DTC Customers' and KPI = 'Repeat_Customer_Count' then 'Customer_Count'
when Type = 'Repeat DTC Customers' and KPI = 'Repeat_Customer_Orders' then 'Orders'
else KPI end as KPI
 from cte
)

select *,'M' as Period from final
union all
select '2023-01-01' as datemonth,'January 2023' as Month,
Business_Unit, Type,sum(VALUE)VALUE,kpi,'Y' as Period
 from final
group by 1,2,3,4,6