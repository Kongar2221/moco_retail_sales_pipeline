{{ 
  config(
    materialized='incremental',
    unique_key=['sale_date','item_code','supplier']
  ) 
}}

with base as (
  select *
  from {{ ref('stg_moco_retail_typed') }}
  where sale_date is not null
  {% if is_incremental() %}
    and sale_date >= (select coalesce(max(sale_date), '1900-01-01') from {{ this }})
  {% endif %}
),
norm as (
  select
    sale_date,
    coalesce(nullif(trim(supplier), ''), 'unknown') as supplier,
    item_code,
    item_description,
    item_type,
    coalesce(retail_sales,0)      as retail_sales,
    coalesce(retail_transfers,0)  as retail_transfers,
    coalesce(warehouse_sales,0)   as warehouse_sales
  from base
),
agg as (
  select
    sale_date,
    supplier,
    item_code,
    item_description,
    item_type,
    sum(retail_sales)                                as retail_sales,
    sum(retail_transfers)                            as retail_transfers,
    sum(warehouse_sales)                             as warehouse_sales,
    sum(retail_sales + retail_transfers + warehouse_sales) as total_sales
  from norm
  group by 1,2,3,4,5
)
select * from agg
