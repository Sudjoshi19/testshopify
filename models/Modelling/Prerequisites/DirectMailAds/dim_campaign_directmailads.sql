      select 
      cast('' as string) as campaign_id, 
      'Direct Mail' as campaign_type, 
      type_ as campaign_name, 
      'Direct Mail' as ad_channel,  
      cast(null as string) as adgroup_id,cast(null as string) as adgroup_name,
      cast(null as string) as ad_id,cast(null as string) as ad_name,
      _daton_batch_runtime,
      row_number() over(partition by type_ order by _daton_batch_runtime desc) as row_num 
      from `weezietowelsdaton.Weezie_raw_data.DirectMailSpend_Direct_Mail_Spend_Sheet1` 