drop table vectortab10korc_txt;
drop table vectortab10korc;

create table vectortab10korc_txt(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
        row format delimited
        fields terminated by '|'
        stored as textfile;

load data local inpath "../../data/files/vectortab10korc.dat" overwrite into table vectortab10korc_txt;


create table vectortab10korc stored as orc as select * from vectortab10korc_txt;

set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.fetch.task.conversion=none;

explain
select count(*) from vectortab10korc
    where (((s LIKE 'a%') or ((s like 'b%') or (s like 'c%'))) or
         ((length(s) < 50 ) and ((s like '%n') and (length(s) > 0))));


select count(*) from vectortab10korc
    where (((s LIKE 'a%') or ((s like 'b%') or (s like 'c%'))) or
         ((length(s) < 50 ) and ((s like '%n') and (length(s) > 0))));

drop table vectortab10korc_txt;
drop table vectortab10korc;

