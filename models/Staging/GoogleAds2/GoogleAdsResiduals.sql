with campaign as (
    select date,campaign_name,ad_channel as CampaignType,
    sum(CLICKS)CLICKS,sum(IMPRESSIONS)IMPRESSIONS,
    sum(CONVERSIONS)CONVERSIONS,SUM(spend)COST,sum(sales) ADSALES
     from {{ ref('GoogleCampaignsFinal') }} 
    GROUP BY 1,2,3
),

adgrp AS 
(
    select date,campaign as campaign_name,CampaignType,
    sum(cast(CLICKS as numeric) )CLICKS,
    sum(cast(IMPRESSIONS as numeric) )IMPRESSIONS,
    sum(cast(CONVERSIONS as numeric) )CONVERSIONS,
    sum(round(cast(cost_micros as numeric)/1000000,2)) as COST,
    sum(CONVERSIONS_VALUE) ADSALES
     from {{ ref('GoogleAdsGrp') }}
    GROUP BY 1,2,3
)

select 
coalesce(a.date,b.date)date,
coalesce(a.campaign_name,b.campaign_name)campaign_name,
coalesce(a.CampaignType,b.CampaignType)CampaignType,
a.CLICKS - coalesce(b.CLICKS,0) as clicks,
a.IMPRESSIONS - coalesce(b.IMPRESSIONS,0) as impressions,
a.CONVERSIONS - coalesce(b.CONVERSIONS,0) as conversions,
a.COST - coalesce(b.COST,0) as spend,
a.ADSALES - coalesce(b.ADSALES,0) as sales
 from campaign a full outer join adgrp b 
on a.date = b.date and a.campaign_name = b.campaign_name and a.CampaignType = b.CampaignType