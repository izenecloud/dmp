-- register the jar file
REGISTER ./dist/pig-udfs.jar

-- alias function name
DEFINE IDENTITY com.b5m.pig.udf.IdentityUDF();

-- load input passed to this script
data = LOAD 'input' as (name:chararray);

-- process data
data2 = FOREACH data GENERATE IDENTITY(name);

-- store output
STORE data2 into 'output' USING PigStorage();

