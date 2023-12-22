WITH a AS (
SELECT 
row_number() OVER (ORDER BY pvm, summa DESC) as rownum,
ref,
pvm, 
summa, 
memo, 
rule
FROM {{ref( 'stg_amex')}}
WHERE rule IS NOT NULL
ORDER BY pvm, summa DESC
)
SELECT a.rownum, a.ref, a.pvm AS date, '7700' as account_number, NULL as dim1, a.summa as amount, a.memo as memo
FROM a
UNION
SELECT a.rownum, a.ref, a.pvm, '2940', NULL, -1 * a.summa, a.memo
FROM a
ORDER BY 1, 2 DESC