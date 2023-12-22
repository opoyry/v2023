SELECT 
tositelaji,
rownum,
-- row_number() OVER (ORDER BY tositelaji, rownum, account) as rownum,
date,
amount,
account,
memo,
dim1
FROM {{ref( 'read_manual_entry_json')}}
ORDER BY rownum, account
