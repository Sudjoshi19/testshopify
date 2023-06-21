with dedup as (

select Fee_Type,date(cast(Start_Date as datetime)) as start_date,coalesce(cast(Fee as numeric),cast(Fee_nu as numeric)) as Fee,Notes,row_number() over(partition by fee_Type,Start_Date order by _daton_batch_runtime desc ) as rownum
from `Weezie_raw_data.Weezie_US_Gsheet_CM_Fee_Saras___Contribution_Margin_Fees_Over_Time_Sheet1`),

dedup_final as (select * except(rownum)
from dedup
where rownum=1
order by 1,2),

final_date as (select  *,lead(Start_Date) over(order by Fee_type,Start_date) as upto_date,
lead(Fee_type) over(order by Fee_type,Start_date) as Next_fee
from dedup_final 
order by 1,2)

select Fee_type,start_date,CASE when fee_type=next_fee then upto_date-1 else '2099-01-01' end as end_Date,fee,notes
from final_date
