{{
  config(
    materialized='table'
  )
}}
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

