with dedup as (
select *,row_number() over(partition by month order by _daton_batch_runtime desc) r
 from weezietowelsdaton.Weezie_raw_data.Goals_Goals_Template___Saras_WHOLESALE
),
cte as (
select * except(_daton_user_id,_daton_batch_runtime,_daton_batch_id,sheetVersionNumber,LastModifiedTime,sheetRowNumber,r),
'Wholesale' as  Business_Unit,'Revenue' as Type 
from dedup where r = 1
)
select * from cte
unpivot(VALUE for KPI in (Gross_Revenue,Discounts,Refunds,Shipping,Net_Revenue) )