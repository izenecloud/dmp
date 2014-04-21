/*
Required parameters:
- model_file    path to the MaxEnt model file
- mode          'mapreduce' (default) or 'local' (only for testing)
- today         today's date in format YYYY-MM-DD (defaults to system current date)
- input         path to input file or directory
- output_dir    path to output directory
- pigdir        path to pig libraries
- udf_file      path to pig-udfs.jar
*/

%default mode mapreduce
%default pigdir /usr/lib/pig
%default output_dir dmp
%default today `date +%Y-%m-%d`

%declare uuid_filter_regex '(undefined|guest|false)'
%declare url_match_regex '.*(taobao.com|tmall.com|yixun.com|jd.com|dangdang.com|suning.com|yhd.com).*'

REGISTER $pigdir/piggybank.jar
REGISTER $pigdir/lib/avro-*.jar
REGISTER $pigdir/lib/jackson-core-asl-*.jar
REGISTER $pigdir/lib/jackson-mapper-asl-*.jar
REGISTER $pigdir/lib/json-simple-*.jar
REGISTER $pigdir/lib/snappy-java-*.jar
REGISTER $udf_file

DEFINE AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();
DEFINE GetCategory com.b5m.pig.udf.GetCategory('$model_file', '$mode');
DEFINE ConvertToMap com.b5m.pig.udf.ConvertToMap();
DEFINE DateStorage com.b5m.pig.udf.DateStorage('$today');
DEFINE PriceRange com.b5m.pig.udf.PriceToRange();

DEFINE category_map_macro(records, title_field, alias)
RETURNS uuid_categories_map {
    uuid_category = FOREACH $records GENERATE uuid, GetCategory($title_field) AS category;
    uuid_category_group = GROUP uuid_category BY (uuid, category);
    uuid_category_count = FOREACH uuid_category_group GENERATE
                group.uuid,
                group.category,
                COUNT(uuid_category) AS counts;
    uuid_categories = GROUP uuid_category_count BY uuid;
    $uuid_categories_map = FOREACH uuid_categories GENERATE
                group AS uuid,
                ConvertToMap(uuid_category_count) AS $alias;
};

records = LOAD '$input/$today' USING AvroStorage();
entries = FOREACH records GENERATE
            args#'uid' AS uuid:chararray,
            args#'dl'  AS url:chararray,
            args#'tt'  AS title:chararray,
            args#'ti'  AS product:chararray,
            args#'pr'  AS price:chararray,
            args#'lt'  AS logtype:chararray,
            args#'ad'  AS actionid:chararray,
            args#'sr'  AS source:chararray;

clean = FILTER entries BY NOT (uuid MATCHES '$uuid_filter_regex');

clean_title = FILTER clean BY title IS NOT NULL;
uuid_page_categories = category_map_macro(clean_title, title, 'category_count');

clean_product = FILTER clean BY product IS NOT NULL;
uuid_product_categories = category_map_macro(clean_product, product, 'category_count');


clean_price = FILTER clean BY price IS NOT NULL;
uuid_price_range = FOREACH clean_price GENERATE uuid AS uuid, PriceRange(price) AS price_range;
uuid_price_group = GROUP uuid_price_range BY (uuid, price_range); 
uuid_price_count = FOREACH uuid_price_group GENERATE group.uuid, group.price_range, COUNT(uuid_price_range) AS counts;      
uuid_prices = GROUP uuid_price_count BY uuid;
uuid_prices_count = FOREACH uuid_prices GENERATE
                group AS uuid,
                ConvertToMap(uuid_price_count) AS price_count;

source_clean = FILTER clean BY ((logtype == '1001') AND ((actionid == '102') OR (actionid == '103')))
               OR ((logtype == '1002') AND ((actionid == '102') OR (actionid == '103')));
source_clean = FILTER source_clean BY source IS NOT NULL;

uuid_source = FOREACH source_clean GENERATE uuid AS uuid, source AS source;
uuid_source_group = GROUP uuid_source BY (uuid, source);
uuid_source_count = FOREACH uuid_source_group GENERATE group.uuid, group.source, COUNT(uuid_source) AS counts;
uuid_sources = GROUP uuid_source_count BY uuid;
uuid_sources_count = FOREACH uuid_sources GENERATE group AS uuid, ConvertToMap (uuid_source_count) AS source_count;

co = COGROUP uuid_prices_count BY (uuid), 
			 uuid_page_categories BY (uuid), 
			 uuid_product_categories BY (uuid),
             uuid_sources_count BY (uuid); 
uuid_profile = FOREACH co GENERATE group AS uuid, 
			 uuid_page_categories.category_count AS page_category_count,
			 uuid_product_categories.category_count AS product_category_count, 
			 uuid_prices_count.price_count AS price_count,
             uuid_sources_count.source_count AS source_count;
			 
STORE uuid_profile INTO '$output_dir' USING DateStorage();
