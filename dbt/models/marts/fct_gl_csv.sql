{{
  config(
    materialized='table',
    post_hook='COPY fct_gl_csv TO \'../2023/gl_csv.csv\' (HEADER, FORCE_QUOTE *, DATEFORMAT \'%d.%m.%Y\', DELIMITER \',\') ' if target.type == 'XXduckdb'  else ''
  )
}}
with cte as (
select 'bank_statement' as source, 'PNK' as tositelaji, rownum, date as pvm, tili, dimensio, summa, ref as muistio
from  {{ ref( 'fct_bank_statement_entry' ) }} a
union
select 'manual_entry', 'KÄT1', rownum, date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'fct_manual_entry' ) }} d
union
select 'manual_entry_json', tositelaji, rownum, date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'fct_manual_entry_json' ) }} e
union
select 'pdf_invoice', 'KÄT3', rownum, date as pvm, account as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'fct_invoice_entry' ) }} b
union
select 'amex', 'KÄT4', rownum, date as pvm, account_number as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'fct_amex_transactions' ) }} c
union
select 'manual_entry', 'KÄT2', rownum, date as pvm, account_number as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'fct_manual_entry_excel' ) }} c
union
select 'e-invoice', 'OL', rownum, date as pvm, account_number as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'fct_maventa_invoice_entry' ) }} c
order by 1, 2, 3
)
select 
tositelaji,
tositelaji || '-' || rownum as "Tosite",
pvm as "Pvm",
tili as "Nro", 
CASE WHEN summa > 0 then summa else 0 end as "Debet",
case when summa < 0 then summa * -1 else 0 end as "Kredit",
muistio as "Selite",
dimensio,
source
from cte
order by tositelaji, rownum, tili