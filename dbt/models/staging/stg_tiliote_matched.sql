SELECT 
{% if target.type == 'duckdb' %}
DISTINCT ON (  a.jarjno, b.kdi ) 
{% endif %}
* 
from  {{ ref( 'raw_tiliote' ) }} a,
 {{ ref( 'seed_vientityyppi' ) }} b
WHERE  (
    ( ifnull( b.similar_vs_like, false ) = false AND a.viite LIKE b.template )
OR    ( ifnull( b.similar_vs_like, false ) = true  AND  
{% if target.type == 'snowflake' %}
 a.viite REGEXP b.template
{% else %}
regexp_matches( a.viite, b.template )
{% endif %}
))
{% if target.type == 'snowflake' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.jarjno, b.kdi ORDER BY a.jarjno, b.kdi) = 1
{% endif %}
ORDER BY  a.jarjno, not ifnull( b.similar_vs_like, false ), b.kdi
