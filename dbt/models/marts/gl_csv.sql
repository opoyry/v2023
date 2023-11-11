{{
  config(
    materialized='view',
    post_hook='' if target.name == 'prod' else 'COPY gl_csv TO \'gl_csv.csv\' (HEADER, DELIMITER \',\')'
  )
}}

select a.pvm, a.tili, a.dimensio, a.summa, concat('{{target.type}}' , a.viite ) as muistio
from  {{ ref( 'stg_tiliote_vienti' ) }} a
union
select b.pvm, b.tili, b.dimensio, b.summa, b.muistio
from  {{ ref( 'raw_manual_vienti' ) }} b
order by 1, 2, 3