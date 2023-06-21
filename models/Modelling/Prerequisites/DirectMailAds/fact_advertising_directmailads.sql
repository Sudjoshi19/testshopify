with calendar as (
 SELECT date FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2023-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
) as date)
,
cte as ( select *,(total_cost/Payback__Days_) as unit_cost from (
    SELECT 
distinct date,Type_,
coalesce(cast(Total_Cost as float64),cast(Total_Cost_in as float64),cast(Total_Cost_st as float64))total_cost,
Payback__Days_,cast(Inhome_Date_ as date)start_date,cast(End_Date_ as date)end_date
 FROM `weezietowelsdaton.Weezie_raw_data.DirectMailSpend_Direct_Mail_Spend_Sheet1` 
 where Inhome_Date_ is not null
) )
,
final as (
select distinct a.date,type_ as campaign_name,b.unit_cost from calendar a 
left join cte b 
on a.date <= b.end_date and a.date >= b.start_date
)

    select 
    cast('' as string) campaign_id,campaign_name,
    cast('' as string) as adgroup_id, cast('' as string) as adgroup_name, 
    '' as ad_id,'' as ad_name,
    cast('' as string) as product_id,'' as sku,
    date,
    0 as clicks,0 as impressions,0 as conversions,
    sum(round(cast(unit_cost as numeric),2)) as spend,
    cast(0 as numeric) as sales,
    1 as exchange_currency_rate,
    cast(null as string) as exchange_currency_code,
    'Shopify' as platform_name,'Direct Mail' as ad_channel,'Direct Mail' as ad_type 
    from final group by 2,9