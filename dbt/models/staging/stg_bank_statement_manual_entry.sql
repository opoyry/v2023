with a as (
    select b.date, b.ref, b.amount, e.account
    from {{ ref( 'bank_statement_manual_entry' ) }} e
    join {{ ref( 'read_bank_statement' ) }} b on b.rownum = e.rownum AND b.ref = e.ref AND b.date = e.date AND b.amount = e.amount
)
select a.date, a.ref, NULL as dimensio, a.account, -1 * a.amount
from a
union all
select a.date, a.ref, NULL as dimensio, '1910', a.amount
from a