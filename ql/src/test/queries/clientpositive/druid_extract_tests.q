set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");


EXPLAIN
SELECT max(added), sum(variation), MONTH(`__time`)
FROM druid_table_1 GROUP BY MONTH(`__time`);


EXPLAIN
SELECT max(added), sum(variation)
FROM druid_table_1 WHERE MONTH(`__time`) = 1;

EXPLAIN
SELECT MONTH(`__time`), count(*) FROM druid_table_1 GROUP BY MONTH(`__time`);

EXPLAIN
SELECT FLOOR_YEAR(`__time`), count(*) FROM druid_table_1 GROUP BY FLOOR_YEAR(`__time`);

EXPLAIN
SELECT FLOOR_MONTH(`__time`), count(*) FROM druid_table_1 GROUP BY FLOOR_MONTH(`__time`);

EXPLAIN
SELECT MONTH(`__time`), count(*), max(added), page
FROM druid_table_1 WHERE page IN ('(274020) Skywalker', '(266) Aline') GROUP BY MONTH(`__time`), page;

EXPLAIN
SELECT MONTH(`__time`), count(*), max(added), page
FROM druid_table_1 WHERE YEAR(`__time`) = 2015 GROUP BY MONTH(`__time`), page;

EXPLAIN
SELECT MONTH(`__time`), count(*), max(added), page FROM druid_table_1 WHERE YEAR(`__time`) < 2016
GROUP BY MONTH(`__time`), page;

EXPLAIN
SELECT YEAR(`__time`), count(*) FROM druid_table_1 WHERE MONTH(`__time`) = 9 GROUP BY YEAR(`__time`);


EXPLAIN
SELECT MONTH(`__time`), count(*), max(added) as a, page
FROM druid_table_1 WHERE YEAR(`__time`) = 2015 GROUP BY MONTH(`__time`), page ORDER BY a DESC LIMIT 2;


EXPLAIN
SELECT MONTH(`__time`), count(*), max(added) as a, page FROM druid_table_1
WHERE YEAR(`__time`) = 2015 AND page IN ('(274020) Skywalker', '(266) Aline' ) AND `__time` < '2210-01-01 00:00:00'
GROUP BY MONTH(`__time`), page ORDER BY page DESC LIMIT 2;

EXPLAIN
SELECT MONTH(`__time`), count(*), max(added) as a, page FROM druid_table_1
WHERE YEAR(`__time`) = 2015 AND page IN ('(274020) Skywalker', '(266) Aline') AND `__time` BETWEEN '1210-01-01 00:00:00' AND '2210-01-01 00:00:00'
GROUP BY MONTH(`__time`), page ORDER BY page DESC LIMIT 2;


EXPLAIN SELECT MONTH(`__time`), count(*), max(added) as a, page FROM druid_table_1
WHERE YEAR(`__time`) = 2015 AND page IN ('(274020) Skywalker', '(266) Aline') AND `__time` IN ('1210-01-01 00:00:00', '2210-01-01 00:00:00')
GROUP BY MONTH(`__time`), page ORDER BY page DESC LIMIT 2;

