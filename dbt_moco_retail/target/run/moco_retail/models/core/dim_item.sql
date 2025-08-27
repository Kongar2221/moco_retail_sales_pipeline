
  
    
    

    create  table
      "moco_retail"."main"."dim_item__dbt_tmp"
  
    as (
      

with base as (
  select
    item_code,
    item_description,
    item_type,
    coalesce(nullif(trim(supplier), ''), 'unknown') as supplier
  from "moco_retail"."main"."stg_moco_retail_typed"
),
dim as (
  select
    item_code,
    max(item_description) as item_description,
    max(item_type)        as item_type,
    max(supplier)         as supplier
  from base
  group by item_code
)
select * from dim
    );
  
  