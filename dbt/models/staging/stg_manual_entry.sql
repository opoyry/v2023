WITH a AS (
SELECT date, account, memo, amount, amount_net, vat
FROM {{ref( 'read_manual_entry')}}
)
SELECT a.date, a.account, NULL as dim1, a.amount_net as amount, a.memo as memo
FROM a
UNION
SELECT a.date, '2939', NULL as dim1, a.vat, a.memo as memo
FROM a
WHERE a.vat != 0
UNION
SELECT a.date, '2940', NULL, -1 * a.amount, a.memo
FROM a
ORDER BY 1, 2 DESC