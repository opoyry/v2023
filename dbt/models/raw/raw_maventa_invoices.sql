{{
config(
  materialized='incremental',
  unique_key=['ref'],
  merge_exclude_columns = ['created_at', 'updated_at'],
  incremental_strategy = "delete+insert",
  on_schema_change="ignore"
  )
}}
SELECT Laskunumero, Tila, Vastaanottokanava, date, due_date, received_at, amount, amount_net, sender, ref
,{{ dbt_utils.generate_surrogate_key(['sender', 'Laskunumero']) }} AS row_id
,{{ dbt_date.now() }}  as updated_at
,{{ dbt_date.now() }} as created_at
from  {{ ref( 'read_maventa_invoices' ) }}
{% if is_incremental() %}
  where received_at > (select max(received_at) from {{ this }})
{% endif %}
ORDER BY received_at