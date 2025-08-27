
        
            delete from "moco_retail"."main"."fct_monthly_sales" as DBT_INCREMENTAL_TARGET
            using "fct_monthly_sales__dbt_tmp20250827223109197505"
            where (
                
                    "fct_monthly_sales__dbt_tmp20250827223109197505".sale_date = DBT_INCREMENTAL_TARGET.sale_date
                    and 
                
                    "fct_monthly_sales__dbt_tmp20250827223109197505".item_code = DBT_INCREMENTAL_TARGET.item_code
                    and 
                
                    "fct_monthly_sales__dbt_tmp20250827223109197505".supplier = DBT_INCREMENTAL_TARGET.supplier
                    
                
                
            );
        
    

    insert into "moco_retail"."main"."fct_monthly_sales" ("sale_date", "supplier", "item_code", "item_description", "item_type", "retail_sales", "retail_transfers", "warehouse_sales", "total_sales")
    (
        select "sale_date", "supplier", "item_code", "item_description", "item_type", "retail_sales", "retail_transfers", "warehouse_sales", "total_sales"
        from "fct_monthly_sales__dbt_tmp20250827223109197505"
    )
  