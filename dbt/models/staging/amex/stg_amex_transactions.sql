WITH a AS (
SELECT pvm, summa, memo, rule
FROM {{ref( 'stg_amex')}}
WHERE rule IS NOT NULL
)
SELECT a.pvm AS date, '7700' as account_number, NULL as dim1, a.summa as amount, a.memo as memo
FROM a
UNION
SELECT a.pvm, '2940', NULL, -1 * a.summa, a.memo
FROM a
ORDER BY 1, 2 DESC