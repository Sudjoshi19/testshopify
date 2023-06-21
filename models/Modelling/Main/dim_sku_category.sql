select * except(r) from (
SELECT 
distinct sku,Description,
Selling_Group,Product_Category,Style,Colorway,
row_number() over(partition by sku order by _daton_batch_runtime desc) r 
FROM `weezietowelsdaton.Weezie_raw_data.ProductFamily_Weezie_Logiwa_Master_Item_Import_Master_Item_File` 
) where r =1