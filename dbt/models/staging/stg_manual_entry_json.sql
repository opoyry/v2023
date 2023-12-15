SELECT 
rownum,
date,
amount,
account,
memo,
dim1
FROM {{ref( 'read_manual_entry_json')}}
