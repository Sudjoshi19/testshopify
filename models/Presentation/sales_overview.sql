with cte as (
    select a.*,b.budget_line_item,c.Embroidered,d.TOTAL_COGS
     from {{ ref('sales_overview_all') }} a
    left join {{ ref('shopify_expense_orders') }} b 
    on a.order_key = b.order_key
    left join {{ ref('dim_embroidered') }} c
    on a.order_key = c.order_key
    left join {{ ref('fact_cogs') }} d
    on a.order_key = d.order_key
),

advertising as (
    select ad_type,date,sum(spend)spend 
    from {{ ref('fact_adv') }}
    group by 1,2
)

select 
platform_name,
coalesce(a.date,b.date,s.date)date,
coalesce(a.Business_Unit,b.ad_type,s.ad_type)Business_Unit,
CustomerType,customer_key,
currency_code,
order_key,Embroidered,
coalesce(quantity,0)quantity,
coalesce(line_items,0)line_items,
coalesce(gross_amount,0)gross_amount,
CAST(coalesce(shipping_price,0) AS FLOAT64)shipping_price,
CAST(coalesce(discounts,0) AS FLOAT64) discounts,
coalesce(total_taxes,0)total_taxes,TOTAL_COGS,
coalesce(refunded_quantity,0)refunded_quantity,
coalesce(refunded_line_items,0)refunded_line_items,
coalesce(refunded_amount,0)refunded_amount,
coalesce(refunded_tax,0)refunded_tax,
coalesce(fees,0)fees,
coalesce(shipping_cost,0)shipping_cost,
coalesce(packaging_costs,0)packaging_costs,
coalesce(round(direct_labor,2),0)direct_labor,
CAST(coalesce(b.spend,s.per_day_spend,0) AS FLOAT64) spend
 from (
select * from cte where budget_line_item = 'Revenue'
) a 
full 
outer join advertising b
on a.date = b.date and a.Business_Unit = b.ad_type
full outer join {{ ref('fact_ad_agency_spend') }} s on 
a.date = s.date and a.Business_Unit = s.ad_type