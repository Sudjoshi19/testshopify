   select 
    cast(tam.Campaign_ID as string) campaign_id,
    cast(tam.Campaign_name as string) as campaign_name,
    cast(tam.Ad_Group_ID as string) as adgroup_id, 
    cast(tam.Ad_Group_Name as string) as adgroup_name,
    cast(ta.AdID as string) as ad_id,
    cast(tam.ad_name as string) as ad_name,
    '' as product_id,
    '' as sku,
    date,
    cast(clicks as numeric) clicks,
    cast(impression as numeric) impressions,
    conversions,
    cast(ta.Cost as numeric) as spend,
    cast(TotalCompletePaymentValue as numeric) as sales,
    cast(1 as numeric) as exchange_currency_rate,
    cast(null as string) as exchange_currency_code,
    'Shopify' as platform_name,
    'Tiktok' as ad_channel,
    'Tiktok' as ad_type 
    from {{ ref('tiktok_ads') }} ta 
    left join {{ ref('tiktok_campaign_mapping') }} tam on cast(ta.AdID as string)=tam.ad_id




