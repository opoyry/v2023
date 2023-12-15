{{
  config(
    materialized='table',
    post_hook='COPY gl_csv TO \'../2023/gl_csv.csv\' (HEADER, FORCE_QUOTE *, DATEFORMAT \'%d.%m.%Y\', DELIMITER \',\') ' if target.type == 'XXduckdb'  else ''
  )
}}
with cte as (
select 'bank_statement' as source, 'PNK' as tositelaji, rownum, date as pvm, tili, dimensio, summa, ref as muistio
from  {{ ref( 'stg_bank_statement_entry' ) }} a
union
select 'manual_entry', 'KÄT1', rownum, date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_manual_entry' ) }} d
union
select 'manual_entry_json', 'KÄT2', rownum, date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_manual_entry_json' ) }} e
union
select 'pdf_invoice', 'KÄT3', rownum, date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_invoice_entry' ) }} b
union
select 'amex', 'KÄT4', rownum, date as pvm, account_number as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_amex_transactions' ) }} c
order by 1, 2, 3
)
select 
tositelaji || '-' || rownum as "Tosite",
pvm as "Pvm",
tili as "Nro", 
CASE WHEN summa > 0 then summa else 0 end as "Debet",
case when summa < 0 then summa * -1 else 0 end as "Kredit",
muistio as "Selite",
source
from cte
order by tositelaji, rownum, tili