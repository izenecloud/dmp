data = LOAD '$input' USING com.b5m.pig.udf.ScdLoader();
data = FOREACH data GENERATE fields#'Title', fields#'Category';
data = FILTER data BY com.b5m.pig.udf.MaxEntPairs(*);

grouped = GROUP data ALL;
count = FOREACH grouped GENERATE COUNT(data);
dump count

-- vim:ft=pig:nospell:
