with sku as ( select DISTINCT ORDER_ID,order_date,SKU,QUANTITY from {{ref('dim_sku_cogs')}}  )
,
COGS as (select * from {{ref('SKU_COGS')}})
,
cte as (
SELECT *,(QUANTITY*COGS) AS TOTAL_COGS 
FROM (
    SELECT a.*,COALESCE(b.COGS,0)COGS FROM SKU A LEFT JOIN COGS B 
    ON A.SKU = B.SKU AND A.order_date >= b.Start_Date and a.order_date <= b.End_Date
    ) )

select order_id,{{ dbt_utils.surrogate_key(['order_id']) }}order_key,sum(TOTAL_COGS) TOTAL_COGS 
from cte group by 1,2