
SELECT 
"Kirjauspäivä"::date as date,
"Verolaji" as verolaji, 
Tapahtuma as tapahtuma,
"Määrä, €" as amount
FROM '{{ var("tax_payment_parquet_file_name") }}'
