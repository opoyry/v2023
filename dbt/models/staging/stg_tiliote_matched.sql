select 
{% if target.name != 'prod' %}
distinct on (  a.jarjno, b.kdi ) 
{% endif %}
* 
from  {{ ref( 'raw_tiliote' ) }} a,
 {{ ref( 'seed_vientityyppi' ) }} b
-- where regexp_full_match( a.viite, b.template )
-- WHERE  a.viite SIMILAR TO b.template
WHERE  (
    ( ifnull( b.similar_vs_like, false ) = false AND a.viite LIKE b.template )
OR    ( ifnull( b.similar_vs_like, false ) = true  AND  
{% if target.name == 'prod' %}
 a.viite REGEXP b.template
{% else %}
regexp_matches( a.viite, b.template ) -- a.viite SIMILAR TO b.template )
{% endif %}
))
{% if target.name == 'prod' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.jarjno, b.kdi ORDER BY a.jarjno, b.kdi) = 1
{% endif %}
ORDER BY  a.jarjno, not ifnull( b.similar_vs_like, false ), b.kdi
