select (cast(ad_group_id as string)) as adgroup_id, ad_group_name as adgroup_name, '' as ad_id, 'Google' as ad_channel, 
'Google' as ad_type, _daton_batch_runtime 
from {{ ref('GoogleAdsShoppingPerformanceView') }}