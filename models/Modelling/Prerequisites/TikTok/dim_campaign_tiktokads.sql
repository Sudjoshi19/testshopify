      select 
      cast(Campaign_ID as string) as campaign_id, 
      'Tiktok' as campaign_type,  
      cast(Campaign_name as string) as campaign_name, 
      'Tiktok'  as ad_channel, 
      cast(Ad_Group_ID as string) as adgroup_id,cast(Ad_Group_Name as string) as adgroup_name,
      cast(ad_id as string) as ad_id,cast(Ad_Name as string) as ad_name,
      _daton_batch_runtime,
      row_number() over(partition by Campaign_ID order by _daton_batch_runtime desc) as row_num 
      from {{ ref('tiktok_campaign_mapping') }} 

