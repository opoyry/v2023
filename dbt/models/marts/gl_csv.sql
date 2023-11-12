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
order by 1, 2, 3