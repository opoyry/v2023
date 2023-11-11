{{
  config(
    materialized='view'
  )
}}

select * 
from  {{ ref( 'raw_tiliote' ) }} a
where not exists (
    select 0
    from {{ ref( 'stg_tiliote_matched' ) }} b
    where a.jarjno = b.jarjno
)