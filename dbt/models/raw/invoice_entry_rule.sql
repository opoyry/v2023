select issuer,
product,
account::varchar(20) as account,
dim1::varchar(20) as dim1
from {{ source( 'external_csv', 'invoice_entry_rule' ) }}
