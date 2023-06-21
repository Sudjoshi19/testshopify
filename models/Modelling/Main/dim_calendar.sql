{{ config(materialized='table')}} 

SELECT date
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2019-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS date