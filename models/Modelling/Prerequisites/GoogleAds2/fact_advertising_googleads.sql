with cte as (
select 
CampaignID as campaign_id,campaign as campaign_name,
cast(adgroupID as string) as adgroup_id,adgroup as adgroup_name, 
cast(adid as string) as ad_id,ad as ad_name,
'' as product_id,'' as sku,
date,
cast(CLICKS as numeric) as clicks,cast(IMPRESSIONS as numeric) as impressions,
cast(CONVERSIONS as numeric) as conversions,
round(cast(cost_micros as numeric)/1000000,2) as spend,CONVERSIONS_VALUE as sales,
cast(1 as numeric) as exchange_currency_rate,
cast(null as string) as exchange_currency_code,
'Shopify' as platform_name,
CampaignType as ad_channel,
'Google' as ad_type
from {{ ref('GoogleAdsGrp') }}

union all 

select 
cast(null as string) as campaign_id,campaign_name,
cast(null as string) as adgroup_id,cast(null as string) as adgroup_name, 
cast(null as string) as ad_id,cast(null as string) as ad_name,
'' as product_id,'' as sku,
date,
clicks,impressions,
conversions,spend,sales,
cast(1 as numeric) as exchange_currency_rate,
cast(null as string) as exchange_currency_code,
'Shopify' as platform_name,
CampaignType as ad_channel,
'Google' as ad_type
FROM {{ ref('GoogleAdsResiduals') }}

)

select 
campaign_id,campaign_name,
adgroup_id,adgroup_name, 
ad_id,ad_name,
product_id,sku,
date,
sum(clicks)clicks,sum(impressions)impressions,sum(conversions)conversions,sum(spend)spend,sum(sales)sales,
exchange_currency_rate,exchange_currency_code,
platform_name,ad_channel,ad_type
from cte
group by 1,2,3,4,5,6,7,8,9,15,16,17,18,19