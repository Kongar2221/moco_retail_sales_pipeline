{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw','raw_moco_retail') }}
),
renamed as (
  select try_cast("YEAR" as integer) as year_num,
         try_cast("MONTH" as integer) as month_num,
         cast("SUPPLIER" as varchar) as supplier,
         cast("ITEM CODE" as varchar) as item_code,
         cast("ITEM DESCRIPTION" as varchar) as item_description,
         cast("ITEM TYPE" as varchar) as item_type,
         try_cast("RETAIL SALES" as double)  as retail_sales,
         try_cast("RETAIL TRANSFERS" as double)  as retail_transfers,
         try_cast("WAREHOUSE SALES" as double)  as warehouse_sales
    from src
),
final as (
  select year_num, month_num, supplier,
         item_code, item_description, item_type,
         retail_sales, retail_transfers, warehouse_sales,
         case
           when year_num between 1900 and 2100 and month_num between 1 and 12
           then make_date(year_num, month_num, 1)
           else null
         end as sale_date
    from renamed
)
select * from final
