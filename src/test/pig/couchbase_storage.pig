%default password ''
%default expire 0
DEFINE CouchbaseStorage com.b5m.pig.udf.CouchbaseStorage('$hosts', '$bucket', '$password', '$expire');

data = LOAD '$input' as (uuid:chararray, date:chararray, period:int, categories:map[int]);
documents = FOREACH data GENERATE uuid, TOTUPLE(*);

STORE documents INTO 'output' USING CouchbaseStorage();

-- vim:ft=pig:nospell: