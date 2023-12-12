{{
  config(
    materialized='table',
    post_hook='COPY gl_csv TO \'../2023/gl_csv.csv\' (HEADER, FORCE_QUOTE *, DATEFORMAT \'%d.%m.%Y\', DELIMITER \',\') ' if target.type == 'XXduckdb'  else ''
  )
}}
with cte as (
select date as pvm, tili, dimensio, summa, ref as muistio
from  {{ ref( 'stg_bank_statement_entry' ) }} a
union
select date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_invoice_entry' ) }} b
union
select date as pvm, account_number as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_amex_transactions' ) }} c
union
select date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_manual_entry' ) }} d
union
select date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'read_manual_entry_json' ) }} e
order by 1, 2, 3
)
select -- row_number() OVER () as "Tosite", 
pvm as "Pvm", tili as "Nro", -- dimensio as "Kohdennus", 
CASE WHEN summa > 0 then summa else 0 end as "Debet",
case when summa < 0 then summa * -1 else 0 end as "Kredit",
muistio as "Selite"
from cte