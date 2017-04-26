set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");

EXPLAIN SELECT MONTH(`__time`), count(*), page FROM druid_table_1
Where page BETWEEN 1 AND 3 GROUP BY MONTH(`__time`), page;

EXPLAIN SELECT MONTH(`__time`), count(*), page FROM druid_table_1
Where page BETWEEN 1.0 AND 3.0 GROUP BY MONTH(`__time`), page;


EXPLAIN SELECT MONTH(`__time`), count(*), page FROM druid_table_1
Where cast(page as float) BETWEEN 1.0 AND 3.0 GROUP BY MONTH(`__time`), page;

EXPLAIN SELECT MONTH(`__time`), count(*), page FROM druid_table_1
Where cast(page as integer) BETWEEN 1 AND 3 GROUP BY MONTH(`__time`), page;

EXPLAIN SELECT page from druid_table_1 WHERE cast(page as integer) < 5 group by page;

-- @TODO this is not pushed as group by need to be fix it is caused by the cast add by hive

EXPLAIN SELECT page from druid_table_1 WHERE page < 5 group by page;

EXPLAIN SELECT page from druid_table_1 WHERE page < 5.0 group by page;

EXPLAIN SELECT page from druid_table_1 WHERE page < '5' group by page;