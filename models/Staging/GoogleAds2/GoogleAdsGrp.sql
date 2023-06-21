with cte as (
    SELECT *, row_number() OVER(PARTITION BY CAMPAIGNID,ADGROUPID,ADID,DATE ORDER BY _daton_batch_runtime DESC) R FROM(
    select a.*except(ad),ad.id as adid,ad.name as ad from
    (
      select a.* except(campaign,ad_group,ad_group_ad,metrics,segments),
      coalesce(cast(campaign.id as string),cast(campaign.id_nu as string) ) as CampaignID,
      campaign.name as campaign,campaign.advertising_channel_type as CampaignType,
      ad_group.ID as adgroupID,ad_group.name as adgroup,
      ad_group_ad.ad,
      metrics.*,segments.*
       from weezietowelsdaton.Weezie_raw_data.GoogleAds_AdGroup a,
      unnest(a.campaign)campaign,
      unnest(a.ad_group)ad_group,
      unnest(a.ad_group_ad)ad_group_ad,
      unnest(a.metrics)metrics,
      unnest(a.segments)segments  
    ) a, unnest(a.ad)ad  )
)

SELECT * FROM cte where r = 1 

