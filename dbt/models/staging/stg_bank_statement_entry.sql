with cte as (
    select a.date, a.ref, c.tili, c.dimensio, -1 * a.amount as summa
    from  {{ ref( 'stg_bank_statement_matched' ) }} a
    inner join {{ ref( 'bank_statement_match' ) }} b on b.kdi = a.kdi
    inner join {{ ref( 'bank_statement_match_entry' ) }} c on c.kdi = b.kdi
    union
    select a.date, a.ref, b.kdi, NULL as dimensio, -1 * a.amount
    from  {{ ref( 'stg_bank_statement_matched' ) }} a
    inner join {{ ref( 'bank_statement_match' ) }} b on b.kdi = a.kdi
    where not exists (
        select 0
        from {{ ref( 'bank_statement_match_entry' ) }} c
        where c.kdi = b.kdi
    )
    union
    select a.date, a.ref, '1910', NULL as dimensio, a.amount
    from  {{ ref( 'stg_bank_statement_matched' ) }} a
)
select date, regexp_replace(ref, '\n', ' ') as ref, tili, dimensio, summa
from cte