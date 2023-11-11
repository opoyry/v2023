with source as (
    select * from {{ ref( 'vientityyppi_vienti' ) }} 
),
renamed as (
    select
        cast( kdi as string ) as kdi,
        cast( tili as string ) as tili,
        cast( dimensio as string ) as dimensio
    from source
)
select * from renamed
