REGISTER ./dist/pig-udfs.jar

%default password ''
%default batchSize 10

DEFINE CouchbaseStorage com.b5m.pig.udf.CouchbaseStorage('$hosts', '$bucket', '$password', '$batchSize');

data = LOAD '$input' as (uuid:chararray, date:chararray, period:int, categories:map[int]);
documents = FOREACH data GENERATE uuid, TOTUPLE(*);

STORE documents INTO 'output' USING CouchbaseStorage();

-- vim: set ft=pig nospell:
