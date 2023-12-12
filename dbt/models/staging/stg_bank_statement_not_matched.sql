{{
  config(
    materialized='view'
  )
}}

select * 
from  {{ ref( 'read_bank_statement' ) }}
where rownum NOT IN (
    select rownum
    from {{ ref( 'stg_bank_statement_matched' ) }}
)
AND rownum NOT IN (
    SELECT rownum
    FROM {{ ref( 'bank_statement_manual_entry' ) }}
)