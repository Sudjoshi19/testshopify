      select 
      cast(campaign_id as string) as campaign_id, 
      'Google' as campaign_type,  
      campaign_name, 
      ad_channel, 
      adgroup_id,adgroup_name,
      ad_id,ad_name,
      1 as _daton_batch_runtime,
      1 as row_num 
      from {{ ref('fact_advertising_googleads') }} 