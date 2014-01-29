/*
Required parameters:
- date
- count
- input         path to input file or directory
- hosts         URIS to couchbase servers
- bucket        couchbase bucket
- password      couchbase password (if required)
- batchSize     couchbase commit batch size
- udf_file      path to pig-udfs.jar
*/


DEFINE Normalize com.b5m.pig.udf.NormalizeMap();
DEFINE Merge com.b5m.pig.udf.MergeMaps();
DEFINE CouchbaseStorage com.b5m.pig.udf.CouchbaseStorage('$hosts', '$bucket', '$password', '$batchSize');

daily = LOAD '$input' as (uuid:chararray, categories:[int]);
grouped = GROUP daily BY uuid;
analytics = FOREACH grouped {
                merged = Merge(daily.categories);
                normalized = Normalize(merged);
                GENERATE group AS uuid, normalized AS categories;
            }
documents = FOREACH analytics GENERATE
                CONCAT(uuid, '::$date') AS key,
                TOTUPLE(uuid, '$date', $count, categories)
                    AS value:(uuid:chararray, date:chararray, period:int, categories:[double]);
STORE documents INTO 'unused' USING CouchbaseStorage();

-- vim:ft=pig:nospell:
