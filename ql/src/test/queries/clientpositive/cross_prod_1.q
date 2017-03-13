set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.tez.cartesian-product.enabled=true;

create table X as
select distinct * from src order by key limit 10;

explain select * from X as A, X as B;
select * from X as A, X as B;

explain select * from X as A join X as B on A.key<B.key;
select * from X as A join X as B on A.key<B.key;

explain select * from X as A join X as B on A.key between "103" and "105";
select * from X as A join X as B on A.key between "103" and "105";

explain select * from X as A, X as B, X as C;
select * from X as A, X as B, X as C;

explain select * from X as A join X as B on A.key in ("103", "104", "105");
select * from X as A join X as B on A.key in ("103", "104", "105");

explain select A.key, count(*)  from X as A, X as B group by A.key;
select A.key, count(*)  from X as A, X as B group by A.key;

explain select * from X as A left outer join X as B on (A.key = B.key or A.value between "val_103" and "val_105");
explain select * from X as A right outer join X as B on (A.key = B.key or A.value between "val_103" and "val_105");
explain select * from X as A full outer join X as B on (A.key = B.key or A.value between "val_103" and "val_105");

explain select * from (select X.key, count(*) from X group by X.key) as A, (select X.key, count(*) from X group by X.key) as B;
select * from (select X.key, count(*) from X group by X.key) as A, (select X.key, count(*) from X group by X.key) as B;

explain select * from (select * from src union all select * from src as y) a join src;
explain select * from (select * from src union all select * from src as y) a join (select * from src union all select * from src as y) b;