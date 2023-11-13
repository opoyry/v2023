    select
{% if target.type == 'snowflake' %}
        cast( $1:c1 as date ) as pvm,
        cast( $1:c2 as string ) as viite,
        cast( $1:c3 as string ) as tili,
        cast( $1:c4 as double ) as summa
{% else %}
        cast( pvm as date ) as pvm,
        cast( viite as string ) as viite,
        cast( tili as string ) as tili,
        cast( summa as double ) as summa
{% endif %}
from {{ source( 'external_s3' if target.name == 'prod' else 'external_csv', 'tiliote_manual_vienti' ) }}
