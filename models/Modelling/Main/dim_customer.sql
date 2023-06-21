       select distinct 
        {{ dbt_utils.surrogate_key(['email']) }} AS customer_key,
        {{ dbt_utils.surrogate_key(['order_id']) }} AS order_key,
        email,order_date,
        MAX(order_date) OVER (PARTITION BY email) AS last_order_date,
        MIN(order_date) OVER (PARTITION BY email) AS acquisition_date,
        acquisition_channel
	      from {{ref('dim_customer_shopify')}} 

