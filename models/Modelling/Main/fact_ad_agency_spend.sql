with dedup_final as (
    select *,coalesce(cast(spend as numeric),cast(Spend_in as numeric)) as final_spend,
    row_number() over(partition by Month order by _daton_batch_runtime) as row_num
    from `Weezie_raw_data.Weezie_US_Gsheet_Ad_Agency_Spend_Saras___Ad_Agency_Spend_Sheet1`
),

dedup as (select Month,final_spend,EXTRACT(DAY FROM LAST_DAY(month)) as daysinmonth,(final_spend/EXTRACT(DAY FROM LAST_DAY(month)) ) per_day_spend
from dedup_final
where row_num=1),

dates as (SELECT date
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2023-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS date )

select 'Agency spend' as ad_type,date,per_day_spend
from dates
join dedup on  date_trunc(dates.date,month)=dedup.month