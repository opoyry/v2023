{{
  config(
    materialized='view'
  )
}}
WITH a AS (
SELECT 
row_number() OVER (ORDER BY date, received_at) as rownum,
Laskunumero, Tila, Vastaanottokanava, date,
 due_date, received_at, amount, amount_net, sender, ref, row_id, updated_at, created_at,
 sender || ' ' || ref as memo,
 ( amount - amount_net ) as amount_vat
FROM {{ref( 'stg_maventa_invoices')}}
ORDER BY date, received_at
)
SELECT a.rownum, a.ref, a.date, '2871' as account_number, 
 {{ normalize_state("sender", 0) }} as dim1, 
 -1 * a.amount as amount, a.memo
FROM a
UNION
SELECT a.rownum, a.ref, a.date, 
 {{ normalize_state("sender", 1) }} as account_number, 
 NULL as dim1, 
 a.amount_net as amount,
a.memo
FROM a
UNION
SELECT a.rownum, a.ref, a.date, '1763', NULL, a.amount_vat as amount, a.memo
FROM a
ORDER BY 1, 2 DESC