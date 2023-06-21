{{config(  materialized='table' )}}
    
    
select *,{{ dbt_utils.surrogate_key(['product_id', 'sku','platform_name']) }} AS product_key, 
{{ dbt_utils.surrogate_key(['sku']) }} AS sku_key
from {{ ref('dim_product_shopify') }}
