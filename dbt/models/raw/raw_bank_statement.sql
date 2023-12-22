{{
config(
  materialized='incremental',
  unique_key=['ref'],
  merge_exclude_columns = ['created_at', 'updated_at'],
  incremental_strategy = "delete+insert",
  on_schema_change="ignore"
  )
}}
SELECT date, date2, amount, ref, rownum, status
,{{ dbt_utils.generate_surrogate_key(['date', 'rownum']) }} AS row_id
,{{ dbt_date.now() }}  as updated_at
-- ,current_date() as created_at
,{{ dbt_date.now() }} as created_at
from  {{ ref( 'read_bank_statement' ) }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where date > (select max(date) from {{ this }})

{% endif %}
ORDER BY date, rownum