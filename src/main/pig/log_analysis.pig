-- Required parameters:
--   model_file     path to the MaxEnt model file
--   mode           'mapreduce' (default) or 'local' (only for testing)
--   today          today's date in format YYYY-MM-DD (defaults to system current date)
--   input          path to input file or directory
--   output_dir     path to output directory
--   pigdir         path to pig libraries
--   udf_file       path to pig-udfs.jar

-- default values
%default mode mapreduce
%default pigdir /usr/lib/pig
%default output_dir dmp
%default today `date +%Y-%m-%d`

-- other parameters
%declare uuid_filter_regex '(undefined|guest|false)'
%declare url_match_regex '.*(taobao.com|tmall.com|yixun.com|jd.com|dangdang.com|suning.com|yhd.com).*'

-- required libraries
REGISTER $pigdir/piggybank.jar
REGISTER $pigdir/lib/avro-*.jar
REGISTER $pigdir/lib/jackson-core-asl-*.jar
REGISTER $pigdir/lib/jackson-mapper-asl-*.jar
REGISTER $pigdir/lib/json-simple-*.jar
REGISTER $pigdir/lib/snappy-java-*.jar
REGISTER $udf_file

-- shorter aliases
DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
DEFINE GetCategory com.b5m.pig.udf.GetCategory('$model_file', '$mode');
DEFINE CategoryMap com.b5m.pig.udf.ConvertToMap();

-- load log files in avro format
records = LOAD '$input' USING AvroStorage();

-- extract only required fields
entries = FOREACH records GENERATE
            args#'uid' AS uuid:chararray,
            args#'dl'  AS url:chararray,
            args#'tt'  AS title:chararray;

-- ignore records without uuid
clean0 = FILTER entries BY NOT (uuid MATCHES '$uuid_filter_regex');
-- ignore records without title
clean1 = FILTER clean0 BY title IS NOT NULL;
-- ignore records not in e-commerce websites
clean2 = FILTER clean1 BY (url MATCHES '$url_match_regex');

/* TODO more filtering here*/

-- classify page using the title
data1 = FOREACH clean2 GENERATE uuid, GetCategory(title) AS category;

-- group by user and category
data2 = GROUP data1 BY (uuid, category);
-- count categories
data3 = FOREACH data2 GENERATE group.uuid, group.category, COUNT(data1) AS counts;
-- group by user
data4 = GROUP data3 BY uuid;

-- generate bag-of-words
data5 = FOREACH data4 GENERATE group AS uuid, CategoryMap(data3) AS categories;

-- store into HDFS
STORE data5 INTO '$output_dir/$today' using JsonStorage();

-- vim: set ft=pig nospell:
