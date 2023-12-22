    select
{% if target.type == 'snowflake' %}
        cast( $1:c1 as date ) as as ronum,
        cast( $1:c2 as date ) as date,
        cast( $1:c3 as string ) as ref,
        cast( $1:c4 as double ) as amount,
        cast( $1:c5 as string ) as account,
        cast( $1:c6 as string ) as dimensio
{% else %}
        rownum,
        cast( date as date ) as date,
        cast( ref as string ) as ref,
        cast( amount as double ) as amount,
        cast( account as string ) as account,
        cast( dimensio as string ) as dimensio
{% endif %}
from {{ source( 'external_s3' if target.name == 'prod' else 'external_csv', 'bank_statement_manual_entry' ) }}
