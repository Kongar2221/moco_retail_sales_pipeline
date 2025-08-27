
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
select
    sale_date, item_code, supplier,
    count(*) as count
from "moco_retail"."main"."fct_monthly_sales"
group by sale_date, item_code, supplier
having count(*) > 1

  
  
      
    ) dbt_internal_test