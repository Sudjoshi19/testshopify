with cte as (
    select distinct order_id,{{ dbt_utils.surrogate_key(['order_id']) }} AS order_key,email,discount_code
    from {{ ref('spy_orders') }} )

 select *,
 case when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'brand_100' then 'Samples & Gifts'
 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'manualorder_100' then 'CX Replacements'
 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'cx_100' then 'CX Surprise & Delight'
 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'donations_100' then 'Donations'
 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'corpgifting_100' then 'Corporate Orders'

 when (lower(email) = 'liz@weezietowels.com' or lower(email) = 'lindsey@weezietowels.com' )
 and lower(discount_code) = 'temp&moose' then 'Orders placed by Co-Founders'

 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'merch_100' then 'Merchandise to be resold'
 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'trunkshow_100' then 'N/A'
 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'replen_100' then 'N/A'
 when lower(email) = 'manualorders@weezietowels.com' and lower(discount_code) = 'display_100' then 'N/A'

 when lower(discount_code) = 'replacement' then 'CX Replacements'
 when lower(discount_code) = 'exchange' then 'CX Exchanges' 
 else 'Revenue' end as budget_line_item 
 from cte