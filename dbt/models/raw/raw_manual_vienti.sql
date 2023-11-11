with source as (
    select * from {{ source( 'external_s3' if target.name == 'prod' else 'external_source', 'manual_journal' ) }}
),
renamed as (
    select
{% if target.name == 'prod' %}
        $1:Account as tili,
        $1:Amount as summa,
        $1:Dim1 as dimensio,
        $1:Memo as muistio,
        $1:Date as pvm
{% else %}
        "Account" as tili,
        "Amount" as summa,
        "Dim1" as dimensio,
        "Memo" as muistio,
        cast( "Date" as date ) as pvm
{% endif %}
    from source
)
select * from renamed
