{{ config(materialized='table') }}

with base as (
  select item_code, item_description,
         item_type, supplier
    from {{ ref('stg_moco_retail_typed') }}
),
dim as (
  select item_code,
         max(item_description) as item_description,
         max(item_type) as item_type,
         max(supplier) as supplier
    from base
   group by item_code
)
select * from dim
