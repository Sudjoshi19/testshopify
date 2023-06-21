select 
  'Shopify' as platform_name,
  _daton_batch_runtime
  from {{ ref('spy_orders') }}