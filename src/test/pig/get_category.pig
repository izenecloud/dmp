%default mode local

DEFINE GET_CATEGORY com.b5m.pig.udf.GetCategory('$model_file', '$mode');

titles = LOAD '$input' AS (title:chararray);
categories = FOREACH titles GENERATE GET_CATEGORY(title);

STORE categories into 'output' USING PigStorage();

-- vim:ft=pig:nospell:
