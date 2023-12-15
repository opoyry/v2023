WITH a AS (
SELECT
    row_number() OVER (ORDER BY date, amount DESC) as rownum,
    date,
    account,
    memo,
    amount,
    amount_net,
    vat
FROM {{ref( 'read_manual_entry')}}
ORDER BY date, amount DESC
)
SELECT rownum, date, account, NULL as dim1, amount_net as amount, memo as memo
FROM a
UNION
SELECT rownum, date, '2939', NULL as dim1, vat, memo as memo
FROM a
WHERE a.vat != 0
UNION
SELECT rownum, date, '2940', NULL, -1 * amount, memo
FROM a
ORDER BY rownum, account