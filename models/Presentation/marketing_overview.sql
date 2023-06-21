{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['date','ad_channel'],
    unique_key = ['date','ad_channel','campaign_name','campaign_type',
                  'adgroup_id','adgroup_name','ad_id','ad_name'])}}

Select 
d.campaign_type as ad_channel,INITCAP(lower(d.ad_channel))campaign_type,
d.campaign_name,d.adgroup_id,d.adgroup_name,d.ad_id,d.ad_name,
date,
sum(spend) adspend,
sum(sales) adsales,
sum(clicks) clicks,
sum(impressions) impressions,
sum(conversions) conversions
from {{ ref('fact_adv')}} a

left join (select distinct campaign_type,ad_channel,campaign_name,campaign_key,
           adgroup_id,adgroup_name,ad_id,ad_name from {{ ref('dim_campaign')}} ) d
on a.campaign_key = d.campaign_key
group by 1,2,3,4,5,6,7,8