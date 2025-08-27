
    
    

select
    item_code as unique_field,
    count(*) as n_records

from "moco_retail"."main"."dim_item"
where item_code is not null
group by item_code
having count(*) > 1


