select distinct order_id,{{ dbt_utils.surrogate_key(['order_id']) }} AS order_key,
Embroidered
 from {{ref('dim_sku_cogs')}} 