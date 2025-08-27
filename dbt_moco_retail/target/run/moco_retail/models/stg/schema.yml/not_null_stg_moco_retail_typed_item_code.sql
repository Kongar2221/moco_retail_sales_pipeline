
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select item_code
from "moco_retail"."main"."stg_moco_retail_typed"
where item_code is null



  
  
      
    ) dbt_internal_test