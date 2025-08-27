
select
    sale_date, item_code, supplier,
    count(*) as count
from "moco_retail"."main"."fct_monthly_sales"
group by sale_date, item_code, supplier
having count(*) > 1
