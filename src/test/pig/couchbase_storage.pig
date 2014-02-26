data = LOAD '$input' as (uuid:chararray, date:chararray, period:int, categories:map[int]);
documents = FOREACH data GENERATE uuid, TOTUPLE(*);

STORE documents INTO 'output' USING com.b5m.pig.udf.CouchbaseStorage('$hosts', '$bucket');

-- vim:ft=pig:nospell:
