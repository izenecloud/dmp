define ScdLoader com.b5m.pig.udf.ScdLoader();
data = LOAD '$input' USING ScdLoader();
grouped = GROUP data ALL;
num = FOREACH grouped GENERATE COUNT(data);
dump num

-- vim:ft=pig:nospell:
