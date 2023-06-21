select 
created_at as order_date,
order_id,
_daton_batch_runtime,
coalesce(lower(email),'') as email,
'ecommerce' as acquisition_channel,
from {{ ref('spy_orders_customer') }}