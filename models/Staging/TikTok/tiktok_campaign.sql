
with dedup as (
select * from
(
    SELECT *,row_number() over(partition by Date, CampaignID order by _daton_batch_runtime desc) as rn FROM 
    weezietowelsdaton.Weezie_raw_data.TikTok_campaign_report_daily
)

where rn=1
)
select * from dedup

