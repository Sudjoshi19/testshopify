select adset_id as adgroup_id, adset_name as adgroup_name, ad_id,
 'Facebook' as ad_channel, 'Facebook' as ad_type, _daton_batch_runtime 
from {{ ref('fb_adinsights') }}