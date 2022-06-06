/*
SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'tesla5forces'  
tbls = ['"COMPETES_WITH"', '"PARENT_OF"', '"Location"', '"HQ_IN"', '"Company"', '"SUPPLIES_TO"', '"_ag_label_vertex"', '"Model"', '"Make"', '"Year"', '"Category"', '"_ag_label_edge"', '"AVAILABLE_IN"', '"CATEGORIZED_AS"', '"MANUFACTURED_BY"']
[print('select * from "tesla5forces".' + tbl + '\nunion') for tbl in  tbls.split('\n')]
*/

create view tesla5forces.nodes AS
select * from "tesla5forces"."Company"
union
select * from "tesla5forces"."Model"
union
select * from "tesla5forces"."Make"
union
select * from "tesla5forces"."Year"
union
select * from "tesla5forces"."Category"

create view tesla5forces.edges AS
select * from "tesla5forces"."COMPETES_WITH"
union
select * from "tesla5forces"."PARENT_OF"
union
select * from "tesla5forces"."Location"
union
select * from "tesla5forces"."HQ_IN"
union
select * from "tesla5forces"."SUPPLIES_TO"
union
select * from "tesla5forces"."AVAILABLE_IN"
union
select * from "tesla5forces"."CATEGORIZED_AS"
union
select * from "tesla5forces"."MANUFACTURED_BY"
;
DELETE FROM public.competitors
WHERE url IN(SELECT url FROM(SELECT  url,ROW_NUMBER() OVER(PARTITION BY body ORDER BY url)
AS row_num FROM public.competitors) t WHERE t.row_num > 1);
--DELETE 579
;

-- connect sites to companies
create or replace view public.sites2companies AS
select compet.*, company.*
from public.competitors compet
inner join "tesla5forces"."Company" company
on position(split_part(compet.parent,'.', 2) in lower(replace(company.properties::varchar,' ','')))<>0
;

--create relationships view
LOAD '$libdir/plugins/age';
SET search_path = ag_catalog, '$user', public;
create or replace view public.v_rels as
select start_id, replace(lower(replace(label::varchar, '_',' ')), '"','') rel, end_id
from cypher('tesla5forces', 
    $$ MATCH (n)-[r]->()
    return label(r), start_id(r), end_id(r)
$$) as (label agtype, start_id agtype, end_id agtype)
;

--create nodes view
LOAD '$libdir/plugins/age';
SET search_path = ag_catalog, '$user', public;
create or replace view public.v_nodes as
select id, replace(labl::varchar, '"', '') labl, split_part(props::varchar,'"', 4) val
from cypher('tesla5forces', 
    $$ MATCH (n)
    return id(n), label(n), properties(n)
$$) as (id agtype, labl agtype, props agtype)
;

-- transform rels into plain text triples
create or replace view public.v_textTriples AS
select n1.labl || ' ' || n1.val || ' ' || rel || ' ' || n2.labl || ' ' || n2.val triples
from public.v_nodes n1
left join public.v_rels r 
    on r.start_id::varchar = n1.id::varchar
left join public.v_nodes n2
    on r.end_id::varchar = n2.id::varchar
;

-- add quintuples
create or replace view public.v_textQuintuples AS
select * from (select
    n1.labl || ' ' || n1.val || ' ' || r.rel || ' ' || n2.labl || ' ' || n2.val  || ' ' 
    || r2.rel || ' ' || n3.labl || ' ' || n3.val quintuples
from public.v_nodes n1
left join public.v_rels r 
    on r.start_id::varchar = n1.id::varchar
left join public.v_nodes n2
    on r.end_id::varchar = n2.id::varchar
left join public.v_rels r2 
    on r2.start_id::varchar = n2.id::varchar
    and r2.rel != r.rel
left join public.v_nodes n3
    on r2.end_id::varchar = n3.id::varchar
) a where quintuples is not null
union 
select * from (select
    n1.labl || ' ' || n1.val || ' ' || r.rel || ' ' || n2.labl || ' ' || n2.val  || ' ' 
    || r2.rel || ' ' || n3.labl || ' ' || n3.val quintuples
from public.v_nodes n1
left join public.v_rels r 
    on r.start_id::varchar = n1.id::varchar
left join public.v_nodes n2
    on r.end_id::varchar = n2.id::varchar
left join public.v_rels r2 
    on r2.end_id::varchar = n2.id::varchar
    and r2.rel != r.rel
left join public.v_nodes n3
    on r2.start_id::varchar = n3.id::varchar
) a where quintuples is not null
UNION
select * from (select
    n1.labl || ' ' || n1.val || ' ' || r.rel || ' ' || n2.labl || ' ' || n2.val  || ' ' 
    || r2.rel || ' ' || n3.labl || ' ' || n3.val quintuples
from public.v_nodes n1
left join public.v_rels r 
    on r.end_id::varchar = n1.id::varchar
left join public.v_nodes n2
    on r.start_id::varchar = n2.id::varchar
left join public.v_rels r2 
    on r2.start_id::varchar = n2.id::varchar
    and r2.rel != r.rel
left join public.v_nodes n3
    on r2.end_id::varchar = n3.id::varchar
) a where quintuples is not null
UNION
select * from (select
    n1.labl || ' ' || n1.val || ' ' || r.rel || ' ' || n2.labl || ' ' || n2.val  || ' ' 
    || r2.rel || ' ' || n3.labl || ' ' || n3.val quintuples
from public.v_nodes n1
left join public.v_rels r 
    on r.end_id::varchar = n1.id::varchar
left join public.v_nodes n2
    on r.start_id::varchar = n2.id::varchar
left join public.v_rels r2 
    on r2.end_id::varchar = n2.id::varchar
    and r2.rel != r.rel
left join public.v_nodes n3
    on r2.start_id::varchar = n3.id::varchar
) a where quintuples is not null

;

create or replace view public.tuples as 
select triples as tuples from public.v_textTriples
UNION
select quintuples as tuples from public.v_textQuintuples
