with cte as (
    select a.*,b.budget_line_item,
    c.* except(sku) 
    from {{ ref('sales_deepdive_all') }} a
    left join {{ ref('shopify_expense_orders') }} b 
    on a.order_key = b.order_key
    left join {{ ref('dim_sku_category') }} c
    on a.sku = c.sku
)

select * from cte where budget_line_item = 'Revenue'
