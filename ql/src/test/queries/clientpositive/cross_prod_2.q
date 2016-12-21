set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.tez.cartesian-product.enabled=true;

create table X as
select distinct * from src order by key limit 10;

create table Y as
select * from src order by key limit 1;

set hive.auto.convert.join.noconditionaltask.size=12;

explain select * from Y, (select * from X as A, X as B) as C where Y.key=C.key;
select * from Y, (select * from X as A, X as B) as C where Y.key=C.key;
