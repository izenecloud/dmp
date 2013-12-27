REGISTER ./dist/pig-udfs.jar

%default password ''

DEFINE CouchbaseStorage com.b5m.pig.udf.CouchbaseStorage('$hosts', '$bucket', '$password');

data = LOAD '$input' as (uuid:chararray, categories:map[int]);

STORE data INTO 'output' USING CouchbaseStorage();

-- vim: set ft=pig nospell:
