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
)
select date, account, dim1, amount, memo
from c
union
select date, '2939', dim1, amount_vat, memo
from c
UNION
select date, '2871', dim1, -1 * ( amount + amount_vat ), memo
from c
order by 1, 3, 5, 4 desc
