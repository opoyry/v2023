SELECT 
{% if target.type == 'duckdb' %}
DISTINCT ON (  a.rownum, b.kdi ) 
{% endif %}
* 
from  {{ ref( 'raw_bank_statement' ) }} a,
 {{ ref( 'bank_statement_match' ) }} b
WHERE  (
    ( ifnull( b.similar_vs_like, false ) = false AND a.ref LIKE b.template )
OR    ( ifnull( b.similar_vs_like, false ) = true  AND  
{% if target.type == 'snowflake' %}
 a.ref REGEXP b.template
{% else %}
regexp_matches( a.ref, b.template )
{% endif %}
))
AND rownum NOT IN (
    SELECT rownum
    FROM {{ ref( 'bank_statement_manual_entry' ) }}
)
{% if target.type == 'snowflake' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.rownum, b.kdi ORDER BY a.rownum, b.kdi) = 1
{% endif %}
ORDER BY  a.rownum, not ifnull( b.similar_vs_like, false ), b.kdi

