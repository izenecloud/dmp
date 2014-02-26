REGISTER ./dist/pig-udfs.jar

DEFINE CouchbaseStorage com.b5m.pig.udf.CouchbaseStorage('$hosts', '$bucket', '$password');

data = LOAD '$input' as (uuid:chararray, date:chararray, period:int, categories:map[int]);
documents = FOREACH data GENERATE uuid, TOTUPLE(*);

STORE documents INTO 'output' USING CouchbaseStorage();

-- vim: set ft=pig nospell:
