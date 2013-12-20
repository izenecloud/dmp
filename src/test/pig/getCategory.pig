-- register the jar file
REGISTER ./dist/pig-udfs.jar

-- alias function name
DEFINE GET_CATEGORY com.b5m.pig.udf.GetCategory('$model_file', 'local');

-- load input passed to this script
titles = LOAD 'input' as (title:chararray);

-- process data
categories = FOREACH titles GENERATE GET_CATEGORY(title);

-- store output
STORE categories into 'output' USING PigStorage();

