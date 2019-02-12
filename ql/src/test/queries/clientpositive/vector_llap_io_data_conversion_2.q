set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;

set hive.vectorized.use.vector.serde.deserialize=true;
--set hive.vectorized.use.row.serde.deserialize=true;

set hive.llap.io.enabled=true;
set hive.llap.io.encode.enabled=true;

drop table if exists varchar_single_partition_test;
drop table if exists varchar_ctas_1_test;

create table varchar_single_partition_test (vs varchar(50)) partitioned by(s varchar(50));
insert into table varchar_single_partition_test partition(s='positive') VALUES ('xavier laertes');

select * from varchar_single_partition_test;

alter table varchar_single_partition_test change column vs vs varchar(10);
select length(vs),reverse(vs) from varchar_single_partition_test where s='positive';
create table varchar_ctas_1_test as select length(vs),reverse(vs) from varchar_single_partition_test where s='positive';

select * from varchar_ctas_1_test;
