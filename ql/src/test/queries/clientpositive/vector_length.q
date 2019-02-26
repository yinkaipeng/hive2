--! qt:dataset:alltypesorc
SET hive.vectorized.execution.enabled=true;

explain extended select count( * ) from alltypesorc 
where (((cstring1 LIKE 'a%') or ((cstring1 like 'b%') or (cstring1 like 'c%'))) or 
((length(cstring1) < 50 ) and ((cstring1 like '%n') and (length(cstring1) > 0))));

explain extended select count( * ) from alltypesorc 
where (((cstring1 LIKE 'a%') or ((cstring1 like 'b%') or (cstring1 like 'c%'))) or (cstring1 like '%n'));
