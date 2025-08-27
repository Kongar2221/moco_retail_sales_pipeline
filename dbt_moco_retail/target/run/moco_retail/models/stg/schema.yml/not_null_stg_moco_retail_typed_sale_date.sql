
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sale_date
from "moco_retail"."main"."stg_moco_retail_typed"
where sale_date is null



  
  
      
    ) dbt_internal_test