select * except(_daton_batch_runtime,r) from (
select *,row_number() over(partition by sku,Start_Date order by _daton_batch_runtime desc) r 
 from (
SELECT 
distinct sku,DATE(Start_Date)Start_Date,
case when end_date = '' or end_date is null then current_date() else date(End_Date) end as End_Date,
COALESCE(CAST(COGS AS NUMERIC),CAST(COGS_IN AS NUMERIC)) COGS,_daton_batch_runtime
 FROM `weezietowelsdaton.Weezie_raw_data.COGS_COGS___Template_Sheet1`
) )where r = 1