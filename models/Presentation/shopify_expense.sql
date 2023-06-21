with cte as (
    select a.*,b.budget_line_item,coalesce(d.TOTAL_COGS,0)COGS
     from {{ ref('sales_overview_all') }} a
    left join {{ ref('shopify_expense_orders') }} b 
    on a.order_key = b.order_key
    left join {{ ref('fact_cogs') }} d
    on a.order_key = d.order_key
)

select * from cte where budget_line_item <> 'Revenue'
