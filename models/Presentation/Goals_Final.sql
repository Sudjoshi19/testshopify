select A.datemonth,a.Period,a.Business_Unit,a.Type,a.KPI,a.VALUE as Actual,
B.VALUE as target,
date(current_timestamp, 'America/New_York') today
 from {{ ref('Goals') }} A 
LEFT JOIN {{ ref('fact_targets') }} B 
ON A.datemonth = date(B.datemonth) AND LOWER(A.Business_Unit) = LOWER(B.Business_Unit)
AND LOWER(A.Type) = LOWER(B.Type) AND LOWER(A.kpi) = LOWER(B.kpi)
and a.Period = b.Period

