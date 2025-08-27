
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select item_code
from "moco_retail"."main"."fct_monthly_sales"
where item_code is null



  
  
      
    ) dbt_internal_test