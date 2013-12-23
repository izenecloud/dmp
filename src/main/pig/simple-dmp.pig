-- Required parameters:
--   model_file     path to the MaxEnt model file
--   mode           'cluster' (default) or 'local' (only for testing)
--   input          path to input file or directory
--   output         path to output file
--   pigdir         path to pig libraries
--   udfdir         path to the pig-udfs.jar

%default mode cluster
%default output output/dmp
%default pigdir /usr/lib/pig
%default udfdir dist

-- required libraries
-- TODO use placeholders so that the script can be pre-processed
REGISTER $pigdir/piggybank.jar
REGISTER $pigdir/lib/avro-*.jar
REGISTER $pigdir/lib/jackson-core-asl-*.jar
REGISTER $pigdir/lib/jackson-mapper-asl-*.jar
REGISTER $pigdir/lib/json-simple-*.jar
REGISTER $pigdir/lib/snappy-java-*.jar
REGISTER $udfdir/pig-udfs.jar

-- shorter aliases
DEFINE GET_CATEGORY com.b5m.pig.udf.GetCategory('$model_file', '$mode');
DEFINE CATEGORY_MAP com.b5m.pig.udf.ConvertToMap();

-- load log files in avro format
records = LOAD '$input' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

-- extract only required fields
entries = FOREACH records GENERATE
            args#'uid' AS uuid:chararray,
            args#'dl'  AS url:chararray,
            args#'tt'  AS title:chararray;

-- ignore records without uuid
clean0 = FILTER entries BY NOT (uuid == 'undefined');
-- ignore records without title
clean1 = FILTER clean0 BY title IS NOT NULL;
-- ignore records not in e-commerce websites
-- TODO more websites here (use parameter)
clean2 = FILTER clean1 BY (url MATCHES '.*(taobao.com|tmall.com|yixun.com|jd.com|dangdang.com|suning.com|yhd.com).*');

-- TODO more filtering here

-- classify page using the title
data1 = FOREACH clean2 GENERATE uuid, GET_CATEGORY(title) AS category;

-- group by user and category
data2 = GROUP data1 BY (uuid, category);
-- count categories
data3 = FOREACH data2 GENERATE group.uuid, group.category, COUNT(data1) AS counts;
-- group by user
data4 = GROUP data3 BY uuid;

-- generate bag-of-words
data5 = FOREACH data4 GENERATE group, CATEGORY_MAP(data3) AS categorycounts;

-- TODO output storage?
STORE data5 INTO '$output';

