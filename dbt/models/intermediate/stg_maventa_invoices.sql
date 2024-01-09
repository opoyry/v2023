{% set fiscalYear = var("fiscalYear")%}
{% set year_start = dbt_date.date(fiscalYear, 1, 1) %}
{% set year_end = dbt_date.date(fiscalYear, 12, 31) %}
SELECT Laskunumero, Tila, Vastaanottokanava, date,
 due_date, received_at, amount, amount_net, sender, ref, row_id, updated_at, created_at
FROM  {{ ref( 'raw_maventa_invoices' ) }} 
WHERE date BETWEEN  '{{ year_start }}' AND '{{ year_end }}'
ORDER BY date, received_at
