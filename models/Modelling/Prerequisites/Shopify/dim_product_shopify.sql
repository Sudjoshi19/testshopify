
select distinct
'Shopify' as platform_name,
cast(product_id as string) product_id,
sku,
title as product_name, 
_daton_batch_runtime
from {{ ref('spy_orders_line_items') }}

