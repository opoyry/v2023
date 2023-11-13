
with cte as (
SELECT
{% if target.type == 'duckdb' %}
    json_extract_string(json_data, 'filename') as filename, 
    json_data ->> '$.issuer' as issuer, 
    json_data ->> '$.invoice_number' as invoice_number, 
    json_data ->> '$.date' as date, 
    case when json_array_length(json_data, 'amount') = 0 then json_data ->>'$.amount' 
    else json_data ->>'$.max_amount'  end::numeric as amount,
    (json_data ->>'$.amount_vat')::numeric as amount_vat,
    json_data ->>'$.product' as product,
    json_extract_string( json_data, 'currency') as currency 
{% elif target.type == 'snowflake' %}
	json_data:issuer::STRING AS issuer,
	json_data:filename::STRING AS filename,
	json_data:invoice_number::STRING AS invoice_number,
	json_data:date::DATE AS date,
	json_data:product::STRING AS product,
	CASE
		WHEN array_size(json_data:amount) IS NULL THEN json_data:amount
		ELSE json_data:max_amount::number(10,
		2)
	END::NUMBER(10, 2) AS amount,
	json_data:amount_vat::NUMBER(10,2) AS amount_vat
{% endif %}
FROM
	invoice_json 
ORDER BY
	3
),
a as (
select cte.date, r.account_number, r.dim1, cte.amount, cte.amount_vat, 
	concat( 'Invoice ', invoice_number, ', file ', filename, ', product ', cte.product) as memo
from cte
inner join {{ ref( 'invoice_entry_rule' ) }} r on cte.issuer = r.issuer and r.product = cte.product
)
select date, account_number, dim1, amount, memo
from a
union
select date, '2939', dim1, amount_vat, memo
from a
UNION
select date, '2871', dim1, -1 * ( amount + amount_vat ), memo
from a
order by 1, 3, 5, 4 desc
