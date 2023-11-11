with source as (
    select * from {{ ref( 'vientityyppi' ) }} 
),
renamed as (
    select
        cast( kdi as string ) as kdi,
        cast( template as string ) as template,
        cast( similar_vs_like as BOOLEAN ) as similar_vs_like
    from source
)
select * from renamed
