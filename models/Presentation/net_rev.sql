with cte as (
    select date,sum(gross_amount)gross_amount, sum(cast(shipping_price as FLOAT64))shipping_price,
    sum(cast(discounts as FLOAT64))discounts, sum(refunded_amount )refunded_amount 
    from {{ ref('sales_overview') }}
    group by 1
)

select date,(gross_amount+shipping_price-discounts+refunded_amount) net_sales from cte
