{{config( 
    materialized='incremental', 
    incremental_strategy='merge', 
    partition_by = { 'field': 'date', 'data_type': 'date' },
    cluster_by = ['date'],
    unique_key = ['date','platform_name','order_channel'])}}


    select 
    c.platform_name,
    order_channel,
    date,
    currency_code,
    sum(quantity) quantity,
    sum(gross_amount) gross_amount,
    sum(shipping_price) shipping_price,
    sum(discounts) discounts,
    sum(total_tax) total_taxes
    from {{ ref('fact_orders') }} a

    left join {{ ref('dim_platform') }} c
    on a.platform_key = c.platform_key
    left join {{ ref('dim_orders') }} d
    on a.order_key = d.order_key
    where transaction_type = 'Order'
    group by 1,2,3,4
