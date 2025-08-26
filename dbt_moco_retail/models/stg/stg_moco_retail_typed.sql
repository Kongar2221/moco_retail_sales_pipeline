{{ config(materialized='view') }}

with typed as (
  {{ moco_typed_select_from_stg() }}
),
final as (
  select
    typed.*,
    case
      when try_cast(year as integer) between 1900 and 2100
       and try_cast(month as integer) between 1 and 12
      then make_date(try_cast(year as integer), try_cast(month as integer), 1)
      else null
    end as sale_date
  from typed
)
select * from final;
