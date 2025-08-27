{{ config(materialized='view') }}
select * from {{ source('raw','raw_moco_retail') }}
 