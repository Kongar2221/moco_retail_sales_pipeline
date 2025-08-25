{{ config(materialized='view') }}
select * from raw_moco_retail
