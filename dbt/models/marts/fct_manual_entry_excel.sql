{{
  config(
    materialized='view'
  )
}}
WITH a AS (
SELECT 
rownum,
date,
debet, 
credit, 
account, 
memo
FROM {{ref( 'read_manual_entry_excel')}}
ORDER BY rownum
)
SELECT
 a.rownum,
 a.date, 
 account as account_number, 
 NULL as dim1, 
 COALESCE( a.debet, -1 * a.credit ) as amount,
a.memo
FROM a
ORDER BY 1, 2 DESC