select distinct order_id,{{ dbt_utils.surrogate_key(['order_id']) }} AS order_key,
  case when lower(discount_code) = 'retailship4h8kp' or (lower(tags) like '%atl retail order%')  then 'Retail'
  when lower(tags) like '%idp%' then 'IDP'
  when lower(tags) like '%wholesale%' then 'wholesale'
  else 'DTC' end as Business_Unit
from {{ ref('spy_orders') }}