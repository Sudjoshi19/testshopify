with dedup as (
select *,row_number() over(partition by month order by _daton_batch_runtime desc) r
 from weezietowelsdaton.Weezie_raw_data.Goals_Goals_Template___Saras_DTC
),
cte as (
select * except(_daton_user_id,_daton_batch_runtime,_daton_batch_id,sheetVersionNumber,LastModifiedTime,sheetRowNumber,r),
from dedup where r = 1
)
select MONTH, 'DTC' AS Business_Unit,
case when KPI like 'Repeat%' then 'Repeat DTC Customers'
when KPI like 'New%' then 'New DTC Customers'
ELSE 'All DTC Customers' end as Type,VALUE,KPI from (
select * from cte
unpivot(VALUE for KPI in (Gross_Revenue,Discounts,Refunds,Shipping,Net_Revenue,Customer_Count,Orders,Net_AOV,
        New_Customer_Net_Revenue,New_Customer_Count,New_Customer_Net_AOV,New_Customer_Orders, 
        Repeat_Customer_Net_Revenue,Repeat_Customer_Count,Repeat_Customer_Net_AOV,Repeat_Customer_Orders ) )
)