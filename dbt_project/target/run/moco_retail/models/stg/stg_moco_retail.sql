
  
  create view "moco_retail"."main"."stg_moco_retail__dbt_tmp" as (
    
select * from raw_moco_retail
  );
