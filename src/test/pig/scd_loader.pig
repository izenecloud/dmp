data = LOAD '$input' USING com.b5m.pig.udf.ScdLoader();
grouped = GROUP data ALL;
num = FOREACH grouped GENERATE COUNT(data);
dump num

-- vim:ft=pig:nospell:
