data = LOAD '$input' USING com.b5m.pig.udf.DateStorage('$date', '$count');
dump data
-- vim:ft=pig:nospell:
