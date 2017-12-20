create database if not exists sbx_millerbarr;

drop table if exists sbx_millerbarr.d_calendar;

create table sbx_millerbarr.d_calendar as
select 1 as intVal, '2' as stringVal
union all
select 1 as intVale, '3' as stringVal;

select * from sbx_millerbarr.d_calendar;

use default;

analyze table sbx_millerbarr.d_calendar compute statistics;

analyze table sbx_millerbarr.d_calendar compute statistics for columns;

