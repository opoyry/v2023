{{
config(
  materialized='incremental',
  unique_key=['ref'],
  merge_exclude_columns = ['created_at', 'updated_at'],
  incremental_strategy = "delete+insert",
  on_schema_change="ignore",
  full_refresh = false
  )
}}
with cte as (
SELECT
Päivämäärä::DATE as date,
Kuvaus as memo1,
"Kortinhaltija," as cardholder,
Tili as account,
Summa as amount,
"Laajennetut tiedot" as memo2,
"Näkyy tiliotteessasi muodossa" as memo3,
Osoite as address,
-- Paikkakunta, 
Postinumero as zip,
Maa as country,
Viite as ref
--, Luokka
from {{ref( 'read_amex')}}
-- from '{{ var("amex.outputFile") }}'
)
SELECT
 date
,memo1
,cardholder
,account
,amount
,memo2
,memo3
,address
,zip
,country
,ref
,{{ dbt_date.now() }}  as updated_at
-- ,current_date() as created_at
,{{ dbt_date.now() }} as created_at
FROM cte
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where date > (select max(date) from {{ this }})

{% endif %}
ORDER BY date, amount DESC