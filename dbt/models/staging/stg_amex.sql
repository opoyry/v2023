{{
  config(
    materialized='table'
  )
}}

WITH a AS (
SELECT pvm, ref, summa, memo
FROM (
  SELECT
  date as pvm,
  ref,
  amount as summa,
  memo1 as memo,
  cardholder as user
  FROM {{ ref('raw_amex') }}
)
WHERE user = 'OLLI POYRY'
),
b AS (

SELECT  *
FROM (
  VALUES
  ('DNS MADE EASY'),
  ('GITHUB, INC.'),
  ('SLACK T057BPLVC3S'),
  ('GOOGLE *GOOGLE STORAGE'),
  ('GOOGLE *GOOGLE PLAY AP  G.CO/HELPPAY#'),
  ('AABACO SMALL BUSINESS   SUNNYVALE'),
  ('MSFT AZURE'),
  ('NAME-CHEAP.COM*'),
  ('PADDLE.NET* HTTP TLKIT  LISBOA'),
  ('AWS EMEA'),
  ('CONFLUENT CLOUD         MOUNTAIN VIEW')
  )
  AS q ( rule )
)
SELECT
  pvm,
  ref,
  summa,
  memo,
  rule
FROM a JOIN b ON a.memo LIKE '%' || b.rule || '%'
UNION
SELECT
  pvm,
  ref,
  summa,
  memo,
  NULL
FROM a 
WHERE NOT EXISTS (
  SELECT 0
  FROM b WHERE a.memo LIKE '%' || b.rule || '%'
)
ORDER BY 1, 2 DESC