select a.pvm, a.viite, a.reference, c.tili, c.dimensio, -1 * a.eur as summa
from  {{ ref( 'stg_tiliote_matched' ) }} a
inner join {{ ref( 'seed_vientityyppi' ) }} b on b.kdi = a.kdi
inner join {{ ref( 'seed_vientityyppi_vienti' ) }} c on c.kdi = b.kdi
union
select a.pvm, a.viite, a.reference, b.kdi, NULL as dimensio, -1 * a.eur
from  {{ ref( 'stg_tiliote_matched' ) }} a
inner join {{ ref( 'seed_vientityyppi' ) }} b on b.kdi = a.kdi
where not exists (
    select 0
    from {{ ref( 'seed_vientityyppi_vienti' ) }} c
    where c.kdi = b.kdi
)
union
select a.pvm, a.viite, a.reference, '1910', NULL as dimensio, a.eur
from  {{ ref( 'stg_tiliote_matched' ) }} a
