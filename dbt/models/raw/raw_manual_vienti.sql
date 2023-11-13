with source as (
    select * from {{ source( 'external_local' if target.type == 'duckdb' else 'external_s3', 'manual_journal_entries' ) }}
),
renamed as (
    select
{% if target.type == 'snowflake' %}
        $1:Account::string as tili,
        $1:Amount::number(10,2) as summa,
        $1:Dim1::string as dimensio,
        $1:Memo::string as muistio,
        $1:Date::DATE as pvm
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
