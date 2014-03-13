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
DEFINE CategoryMap com.b5m.pig.udf.ConvertToMap();
DEFINE DateStorage com.b5m.pig.udf.DateStorage('$today');

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
                CategoryMap(uuid_category_count) AS $alias;
};

records = LOAD '$input' USING AvroStorage();
entries = FOREACH records GENERATE
            args#'uid' AS uuid:chararray,
            args#'dl'  AS url:chararray,
            args#'tt'  AS title:chararray,
            args#'ti'  AS product:chararray,
            args#'pr'  AS price:chararray,
            args#'sr'  AS source:chararray;

clean = FILTER entries BY NOT (uuid MATCHES '$uuid_filter_regex');
clean = FILTER clean BY (url MATCHES '$url_match_regex');

clean_title = FILTER clean BY title IS NOT NULL;
uuid_page_categories = category_map_macro(clean_title, title, 'page_categories');
store uuid_page_categories into 'pages';

clean_product = FILTER clean BY product IS NOT NULL;
uuid_product_categories = category_map_macro(clean_product, product, 'product_categories');
store uuid_product_categories into 'products';

uuid_categories_joined = JOIN uuid_page_categories BY uuid FULL OUTER, uuid_product_categories BY uuid;
describe uuid_categories_joined

/*uuid_categories = FOREACH uuid_categories_grouped GENERATE*/
                    /*group AS uuid,*/
                    /*flatten(uuid_page_categories) AS page_categories,*/
                    /*flatten(uuid_product_categories) AS product_categories;*/
/*describe uuid_categories*/

/*
STORE uuid_categories INTO '$output_dir' USING DateStorage();
*/

-- vim:ft=pig:nospell:
