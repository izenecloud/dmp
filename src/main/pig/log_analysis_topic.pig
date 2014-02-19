-- Required parameters:
--   today          today's date in format YYYY-MM-DD (defaults to system current date)
--   input          path to input file or directory
--   output_dir     path to output directory
--   pigdir         path to pig libraries
--   udf_file       path to pig-udfs.jar
--   topic_jar      path to topic-model.jar
--   num_topic      the number of topic
--   max_iteration  the max iteration number
--   num_topic_doc  the number of topics for one document

-- default values
%default mode mapreduce
%default pigdir /usr/lib/pig
%default output_dir dmp
%default today `date +%Y-%m-%d`

-- other parameters
%declare uuid_filter_regex '(undefined|guest|false)'

-- required libraries
REGISTER $pigdir/piggybank.jar
REGISTER $pigdir/lib/avro-*.jar

-- shorter aliases
DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

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

-- only uuid and title
title = FOREACH clean1 GENERATE uuid, title;
-- group by uuid
uuid = GROUP title BY uuid;
doc = FOREACH uuid GENERATE group, title.title;
/* TODO LOAD function*/
doctopic = MAPREDUCE '$topic_jar' STORE doc INTO 'TEMP/documents' 
            LOAD 'TEMP/res' AS (doc:chararray, topic:chararray) 
            `com.b5m.topic.Extractor TEMP/documents TEMP/res num_topic max_iteration num_topic_doc topic dict`;
-- vim: set ft=pig nospell:
