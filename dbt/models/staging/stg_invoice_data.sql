
with cte as (
SELECT
{% if target.type == 'duckdb' %}
    filename,
    issuer, 
    invoice_number::varchar( 100 ) invoice_number, 
    date, 
    coalesce( max_amount, amount )::numeric amount,
    coalesce( amount_vat, 0)::numeric amount_vat,
    replace( product::varchar( 100 ), '"', '') product, -- replace('[', '').replace(']', '') 
    currency 
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
	'/Users/olli/oma/v2023/2023/pdf-invoices.json'
ORDER BY
	3
)
select * 
from cte