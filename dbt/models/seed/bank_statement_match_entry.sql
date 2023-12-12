with source as (
    select * from {{ source(  'external_csv', 'bank_statement_match_entry' ) }} 
),
renamed as (
    select
        cast( kdi as string ) as kdi,
        cast( tili as string ) as tili,
        cast( dimensio as string ) as dimensio
    from source
)
select * from renamed
