----------------
LOAD '$libdir/plugins/age';
SET search_path = ag_catalog, '$user', public;
select *
from cypher('tesla5forces', 
    $$ MATCH (c)
    return count(c)
$$) as (n agtype)
;
----
"n"
"1327"5

----------------
select *
from cypher('tesla5forces', 
    $$ MATCH (c)
    return label(c), count(*) as ct
$$) as (label agtype, count agtype)
----
"label"	"count"
"""Year"""	"31"
"""Category"""	"8"
"""Make"""	"64"
"""Model"""	"1222"
"""Company"""	"172"

----------------
select *
from cypher('tesla5forces', $$ 
    MATCH (n)-[r]->()
    RETURN label(r), count(*) as ct
$$) as (rel_type agtype, count agtype)
----
"rel_type"	"count"
"""MANUFACTURED_BY"""	"1265"
"""AVAILABLE_IN"""	"9730"
"""CATEGORIZED_AS"""	"1524"
"""SUPPLIES_TO"""	"172"


----------------
create or replace view
public.nodes as
select * from tesla5forces."Category"
union 
select * from tesla5forces."Company"
union 
select * from tesla5forces."Location"
union 
select * from tesla5forces."Make"
union 
select * from tesla5forces."Model"
union 
select * from tesla5forces."Year"
----
create or replace view
public.edges as
select * from tesla5forces."AVAILABLE_IN"
union 
select * from tesla5forces."CATEGORIZED_AS"
union 
select * from tesla5forces."COMPETES_WITH"
union 
select * from tesla5forces."MANUFACTURED_BY"
union 
select * from tesla5forces."PARENT_OF"
union 
select * from tesla5forces."SUPPLIES_TO"