{{
  config(
    materialized='table',
    post_hook='COPY gl_csv TO \'gl_csv.csv\' (HEADER, DELIMITER \',\')' if target.type == 'duckdb'  else ''
  )
}}
select a.pvm, a.tili, a.dimensio, a.summa, a.viite as muistio
from  {{ ref( 'stg_tiliote_vienti' ) }} a
union
select b.pvm, b.tili, b.dimensio, b.summa, b.muistio
from  {{ ref( 'raw_manual_vienti' ) }} b
union
select date as pvm, account_number as tili, dim1 as dimensio, amount as summa, memo as muistio
from  {{ ref( 'stg_invoice_vienti' ) }} c
order by 1, 2, 3