with source as (
    select * from {{ source(  'external_csv', 'bank_statement_match' ) }} 
),
renamed as (
    select
        cast( id as int ) as id,
        cast( kdi as string ) as kdi,
        cast( template as string ) as template,
        cast( similar_vs_like as BOOLEAN ) as similar_vs_like,
        cast( dim1 as string ) as dim1
    from source
)
select * from renamed
