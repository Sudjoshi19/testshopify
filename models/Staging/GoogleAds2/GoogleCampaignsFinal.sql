with cte as (
    select 
    cast(campaign_id as string) campaign_id,
    campaign_name,
    '' as adgroup_id, 
    '' as adgroup_name, 
    '' as ad_id,
    '' as product_id,
    '' as sku,
    date,
    cast(clicks as numeric) clicks,
    cast(impressions as numeric) impressions,
    conversions,
    round((cast(cost_micros as numeric)/1000000),2) as spend,
    conversions_value as sales,
    exchange_currency_rate,
    exchange_currency_code,
    'Shopify' as platform_name,
    campaign_advertising_channel_type as ad_channel,
    'Google' as ad_type 
    from (select * from {{ ref('GoogleAdsCampaign') }} WHERE DATE < '2023-01-01' union all 
          select * from {{ ref('GoogleAds2Campaign') }} WHERE DATE >= '2023-01-01' )
)

select campaign_id,campaign_name,adgroup_id,adgroup_name,ad_id,
       product_id,sku,date,
       sum(clicks)clicks,sum(impressions)impressions,sum(conversions)conversions,
       sum(spend)spend,sum(sales)sales,
       exchange_currency_rate,exchange_currency_code,
       platform_name,ad_channel,ad_type 
       from cte group by 1,2,3,4,5,6,7,8,14,15,16,17,18

