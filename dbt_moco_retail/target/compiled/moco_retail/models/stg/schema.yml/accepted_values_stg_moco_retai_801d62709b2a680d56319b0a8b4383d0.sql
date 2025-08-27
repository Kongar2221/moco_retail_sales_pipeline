
    
    

with all_values as (

    select
        month_num as value_field,
        count(*) as n_records

    from "moco_retail"."main"."stg_moco_retail_typed"
    group by month_num

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)


