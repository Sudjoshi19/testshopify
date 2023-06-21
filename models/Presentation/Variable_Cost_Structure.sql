select 'COGS' as KPI,date,SUM(TOTAL_COGS) AS Cost
from {{ ref('sales_overview') }}
group by 1,2
union all
select 'Discounts',date,SUM(discounts)
from {{ ref('sales_overview') }}
group by 1,2
union all
select 'Fees',date,SUM(fees)
from {{ ref('sales_overview') }}
group by 1,2
union all
select 'Acqusition cost',date,SUM(spend)
from {{ ref('sales_overview') }}
where business_unit<>'Agency spend'
group by 1,2
union all 
select 'Shipping Cost',date,SUM(shipping_cost)
from {{ ref('sales_overview') }}
group by 1,2
union all 
select 'Packaging Costs',date,SUM(packaging_costs)
from {{ ref('sales_overview') }}
group by 1,2
union all 
select 'Direct Labor',date,SUM(direct_labor)
from {{ ref('sales_overview') }}
group by 1,2


