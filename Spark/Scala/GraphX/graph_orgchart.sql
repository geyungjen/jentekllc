--org chart

create table vertex
(
id bigint,
property_name  text,
property_title text
);

create table edge
(
src_id bigint,
dest_id bigint,
relationship  text
);

insert into vertex values (1,'Jack','owner'),(2, 'George', 'clerk'), (3, 'Mary', 'Sales’), (4, ‘Shrry’, ‘wife of owner’);

insert into edge values (1,2,'boss'), (1,3,'boss'), (2,3,'coworker’),(4,1,’boss’);

-- here is the Graph query

with x as (SELECT e.src_id, e.dest_id, e.relationship,
src.property_name src_name,
src.property_title src_title,
dst.property_name dest_name,
dst.property_title dest_title
FROM edge AS e LEFT JOIN vertex AS src ON e.src_id = src.id
LEFT JOIN vertex AS dst ON e.dest_id = dst.id)
select src_name || ', ' || src_title || ', is ‘
                || relationship || ' of ' || dest_name || ', ‘
                || dest_title
from x;


