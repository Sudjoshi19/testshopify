with product_map as (
select distinct sku,title from {{ref('spy_orders_line_items')}}
)
select a.*,b.title from (
select distinct sku from {{ref('dim_sku_cogs')}} 
where sku not in 
( select distinct sku from {{ref('SKU_COGS')}} )
) a 
left join product_map b on a.sku = b.sku
WHERE ( A.SKU <> '' AND A.SKU <> 'null' AND A.sku IS NOT null ) 