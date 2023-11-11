with source as (
    select * from {{ source(  'external_s3' if target.name == 'prod' else 'external_source', 'bank_statement' ) }}
),
renamed as (
    select
{% if target.name == 'prod' %}
        $1:Rivino as jarjno,
        DATE( $1:"Kirjauspäivä", 'DD.MM.YYYY' ) as pvm,
        DATE( $1:"Arvopäivä", 'DD.MM.YYYY' ) as arvopvm,
        $1:"Viite/viesti" as viite,
        cast($1:"Määrä EUR" as double) eur,
        $1:Arkistointitunnus as reference,
        $1:Tila as tila
{% else %}
        ROW_NUMBER() over () as jarjno,
        strptime( "Kirjauspäivä", '%d.%m.%Y' )::DATE as pvm,
        strptime( "Arvopäivä", '%d.%m.%Y' )::DATE as arvopvm,
        left( regexp_replace( regexp_replace( "Viite/viesti", '\n', ' ', 'g'), '\r', ' ', 'g' ), 40 ) as viite,
        cast( "Määrä EUR" as double ) as eur,
        "Arkistointitunnus" as reference,
        "Tila" as tila
{% endif %}
    from source
    order by 1
)
select * from renamed
