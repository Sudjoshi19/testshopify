
select
'Shopify' as order_platform,
order_id,
  'Online Store' as order_channel,
_daton_batch_runtime
from {{ ref('spy_orders') }} 



