-- Shopify Forward Fees
    select 
    date(processed_at) as date,
    'Fees' as amount_type,
    'Order' as transaction_type,
    coalesce(type,'') charge_type,
    cast(source_order_id as string) order_id,
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(fee as numeric)/exchange_currency_rate),2)) amount 
    from {{ ref('spy_transactions')}}
    where type != 'refund' or type != 'payout'
    group by 1,2,3,4,5,6,7,8,9

    UNION ALL

-- Shopify Forward Item Price
    select 
    date(created_at) as date,
    'Revenue' as amount_type,
    'Order' as transaction_type,
    'Item Price' as charge_type,
    order_id,
    cast(product_id as string) product_id,
    sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((CAST(price*quantity as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('spy_orders_line_items')}}
    group by 1,2,3,4,5,6,7,8,9

    UNION ALL

-- Shopify Forward Taxes
    select 
    date(created_at) as date,
    'Taxes' as amount_type,
    'Order' as transaction_type,
    'Taxes' as charge_type,
    order_id,
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(total_tax as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('spy_orders')}}
    group by 1,2,3,4,5,6,7,8,9

    UNION ALL
    
-- Shopify Forward Shipping    
    select 
    date(created_at) as date,
    'Shipping' as amount_type,
    'Order' as transaction_type,
    'Shipping' as charge_type,
    order_id,
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round(((cast(discounted_price as numeric))/exchange_currency_rate),2)) amount
    from {{ ref('spy_orders_shipping_lines')}}
    group by 1,2,3,4,5,6,7,8,9

    UNION ALL

    -- Shopify Forward Shipping Promotions
    select 
    date(created_at) as date,
    'Promotion' as amount_type,
    'Order' as transaction_type,
    'Shipping Promotional Discount' as charge_type,
    order_id,
    '' product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(discount_amount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('spy_orders')}}
    where discount_type = 'shipping'
    group by 1,2,3,4,5,6,7,8,9

    UNION ALL

-- Shopify Forward Item Promotions
    select 
    date(created_at) as date,
    'Promotion' as amount_type,
    'Order' as transaction_type,
    'Promotional Discount' as charge_type,
    order_id,
    '' product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(discount_amount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('spy_orders')}}
    where discount_type != 'shipping'
    group by 1,2,3,4,5,6,7,8,9


    UNION ALL

-- Shopify Reverse Fees
    select 
    date(processed_at) as date,
    'Fees' as amount_type,
    'Refund' as transaction_type,
    coalesce(type,'') charge_type,
    cast(source_order_id as string) order_id,
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(fee as numeric)/exchange_currency_rate),2)) amount 
    from {{ ref('spy_transactions')}}
    where type = 'refund'
    group by 1,2,3,4,5,6,7,8,9

    UNION ALL

-- Shopify Reverse Promotions 
    select 
    date(created_at) as date,
    'Promotion' as amount_type,
    'Refund' as transaction_type,
    'Promotional Discount' as charge_type,
    cast(refunds_order_id as string) order_id,
    cast(product_id as string) product_id,
    sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(total_discount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('spy_orders_refunds_line_items')}}
    group by 1,2,3,4,5,6,7,8,9


    UNION ALL

-- Shopify Reverse Item Price and Shipping
    select 
    date(refunds_created_at) as date,
    'Revenue' as amount_type,
    'Refund' as transaction_type,
    'Refund Revenue' as charge_type,
    cast(refunds_order_id as string) as order_id,
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(transactions_amount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('spy_refunds_transactions')}}
    where refunds_created_at is not null
    group by 1,2,3,4,5,6,7,8,9


    -- UNION ALL

    -- select 
    -- brand,
    -- date(created_at) as date,
    -- 'Revenue' as amount_type,
    -- 'Refund' as transaction_type,
    -- 'Item Price' as charge_type,
    -- order_id,
    -- {#{country('store')}#},
    -- cast(product_id as string) product_id,
    -- sku,
    -- exchange_currency_code,
    -- 'Shopify' as platform_name,
    -- sum(round((cast(price*quantity as numeric)/exchange_currency_rate),2)) amount
    -- from {#{ ref('spy_orders_refunds_line_items')}#}
    -- group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

--  Shopify Reverse Taxes
    select 
    date(refunds_created_at) as date,
    'Taxes' as amount_type,
    'Refund' as transaction_type,
    'Shopify Taxes' as charge_type,
    cast(refunds_order_id as string) order_id,
    cast(product_id as string) product_id,
    sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(line_items_tax_lines_price as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('spy_orders_refunds_tax_lines')}} 
    group by 1,2,3,4,5,6,7,8,9

    -- UNION ALL

    -- -- Shopify Reverse Shipping    
    -- select 
    -- brand,
    -- date(created_at) as date,
    -- 'Shipping' as amount_type,
    -- 'Refund' as transaction_type,
    -- 'Shipping' as charge_type,
    -- a.order_id,
    -- {#{country('store')}#},
    -- '' as product_id,
    -- '' as sku,
    -- exchange_currency_code,
    -- 'Shopify' as platform_name,
    -- sum(round((cast(discounted_price as numeric)/exchange_currency_rate),2)) amount
    -- from {#{ ref('spy_orders_shipping_lines')}} a join (select distinct cast(refunds_order_id as string) refunds_order_id from {{ ref('Shopify_Refunds_transactions')}#}) b
    -- on a.order_id = b.refunds_order_id
    -- group by 1,2,3,4,5,6,7,8,9,10,11