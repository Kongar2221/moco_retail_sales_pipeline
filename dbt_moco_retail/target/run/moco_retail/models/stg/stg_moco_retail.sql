
  
  create view "moco_retail"."main"."stg_moco_retail__dbt_tmp" as (
    
select * from "moco_retail"."main"."raw_moco_retail"
  );
