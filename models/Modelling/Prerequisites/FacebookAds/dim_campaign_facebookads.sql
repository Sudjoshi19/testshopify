      select 
      cast(campaign_id as string) as campaign_id, 
      'Facebook' as campaign_type, 
      campaign_name, 
      'Facebook' as ad_channel,  
       cast(adset_id as string) as adgroup_id,cast(adset_name as string) as adgroup_name,
      cast(ad_id as string) as ad_id,cast(ad_name as string) as ad_name,
      _daton_batch_runtime,
      row_number() over(partition by campaign_id, campaign_name order by _daton_batch_runtime desc) as row_num 
      from {{ ref('fb_adinsights') }}