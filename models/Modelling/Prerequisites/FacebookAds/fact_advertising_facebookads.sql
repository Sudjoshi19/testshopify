with parent as (
select 
campaign_id,
campaign_name,
adset_id, 
adset_name, 
ad_id,
ad_name,
date(date_start) as date,
sum(clicks) clicks,
sum(cast(impressions as numeric)) as impressions,
sum(spend) spend,
exchange_currency_rate,
exchange_currency_code
from {{ref('fb_adinsights')}}
group by 1,2,3,4,5,6,7,11,12
),

child as (
select 
campaign_id,
campaign_name,
adset_id, 
adset_name, 
ad_id,
ad_name,
date(date_start) as date,
sum(case when campaign_name = 'Adhoc_US_BOF_DPA' or campaign_name = 'AdHoc_US_TOF_DABA'
    then 0 else cast(action_values_value as numeric) end ) as sales
from {{ref('fb_adinsights_action_values')}}
where action_values_action_type = 'offsite_conversion.fb_pixel_purchase'
group by 1,2,3,4,5,6,7),

child2 as (
select 
campaign_id,
campaign_name,
adset_id, 
adset_name, 
ad_id,ad_name,
date(date_start) as date,
sum(case when campaign_name = 'Adhoc_US_BOF_DPA' or campaign_name = 'AdHoc_US_TOF_DABA'
    then 0 else cast(actions_value as numeric) end )  as conversions
from {{ref('fb_adinsights_action')}}
where actions_action_type = 'offsite_conversion.fb_pixel_purchase'
group by 1,2,3,4,5,6,7)

select 
a.campaign_id,
a.campaign_name,
a.adset_id as adgroup_id, 
a.adset_name as adgroup_name, 
a.ad_id,a.ad_name,
'' as product_id,
'' as sku,
a.date,
clicks,
impressions,
conversions,
spend,
sales,
exchange_currency_rate,
exchange_currency_code,
'Shopify' as platform_name,
'Facebook' as ad_channel,
'Facebook' as ad_type
from parent a left join child b
on a.campaign_id = b.campaign_id and a.adset_id = b.adset_id and a.adset_name = b.adset_name 
and a.ad_id = b.ad_id and a.ad_name = b.ad_name and a.date = b.date 
left join child2 c
on a.campaign_id = c.campaign_id and a.adset_id = c.adset_id and a.adset_name = c.adset_name 
and a.ad_id = c.ad_id and a.ad_name = c.ad_name and a.date = c.date 