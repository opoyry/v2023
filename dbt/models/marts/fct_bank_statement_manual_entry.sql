with a as (
    select b.rownum, b.date, b.ref, b.amount, e.account, e.dimensio
    from {{ ref( 'bank_statement_manual_entry' ) }} e
    join {{ ref( 'read_bank_statement' ) }} b on b.rownum = e.rownum AND b.ref = e.ref AND b.date = e.date AND b.amount = e.amount
)
select rownum, date, ref, account, dimensio, amount
from a
