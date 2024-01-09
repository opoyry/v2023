with a as (
  select a.rownum, a.date, a.ref, c.tili, c.dimensio, a.amount as summa, b.id as match_id, null as manual_rownum
    from  {{ ref( 'stg_bank_statement_matched' ) }} a
    inner join {{ ref( 'bank_statement_match' ) }} b on b.id = a.id
    inner join {{ ref( 'bank_statement_match_entry' ) }} c on c.kdi = b.kdi
    union
    select a.rownum, a.date, a.ref, b.kdi, b.dim1 as dimensio, a.amount, b.id as match_id, null as manual_rownum
    from  {{ ref( 'stg_bank_statement_matched' ) }} a
    inner join {{ ref( 'bank_statement_match' ) }} b on b.id = a.id
    where not exists (
        select 0
        from {{ ref( 'bank_statement_match_entry' ) }} c
        where c.kdi = b.kdi
    )
    union
    select rownum, date, ref, account, dimensio, amount, null as match_id, rownum as manual_rownum
    from {{ ref( 'fct_bank_statement_manual_entry' ) }}
),
b as (
    select 
        rownum as original_rownum,
        row_number() OVER (ORDER BY date, rownum DESC) as rownum,
        date, 
        ref, 
        tili, 
        dimensio, 
        summa,
        match_id,
        manual_rownum
    from a
    order by date, rownum
),
c as (
    select original_rownum, rownum, date, ref, tili, dimensio, -1 * summa as summa, match_id, manual_rownum
    from b
    union
    select original_rownum, rownum, date, ref, '1910', NULL, summa, match_id, manual_rownum
    from b
)
select original_rownum, rownum, date, regexp_replace(ref, '\n', ' ') as ref, tili, dimensio, summa, match_id, manual_rownum
from c
order by date, original_rownum