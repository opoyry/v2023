with cte as (
	SELECT *
	FROM  {{ ref( 'stg_invoice_data' ) }}
),
a as (
	select cte.date, r.account, r.dim1 as dim1, cte.amount as amount, cte.amount_vat, 
		concat( 'Invoice ', invoice_number, ', file ', filename, ', product ', cte.product) as memo
	from cte
	inner join {{ ref( 'invoice_entry_rule' ) }} r on cte.issuer = r.issuer and r.product = cte.product
),
b as (
	select cte.date, '8500', replace(lower(cte.issuer), ' oyj', '' )  as dim1, cte.amount as amount, cte.amount_vat, 
		concat( 'Invoice ', invoice_number, ', file ', filename, ', product ', cte.product) as memo
	from cte
	where not exists (
		select 1
		from {{ ref( 'invoice_entry_rule' ) }} r 
		where cte.issuer = r.issuer 
		and r.product = cte.product
	)
),
c as (
	select *
	from a
	union
	select *
	from b
),
d as (
	select 
		row_number() OVER (ORDER BY date, amount DESC) as rownum,
		date, account, dim1, amount, amount_vat, memo
		from c
)
select rownum, date, account, dim1, amount, memo
from d
union
select rownum, date, '2939', dim1, amount_vat, memo
from d
UNION
select rownum, date, '2871', dim1, -1 * ( amount + amount_vat ), memo
from d
order by 1, 3
