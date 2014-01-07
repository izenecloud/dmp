-- register the jar file
REGISTER ./dist/pig-udfs.jar

-- alias function name
DEFINE GET_CATEGORY com.b5m.pig.udf.GetCategory('Model.txt');

-- cleanup
rmf output/categories

-- load input passed to this script
titles = LOAD 'input' as (title:chararray);

-- process data
categories = FOREACH titles GENERATE GET_CATEGORY(title);

-- store output
STORE categories into 'output/categories' USING PigStorage();

-- vim: set ft=pig nospell:

