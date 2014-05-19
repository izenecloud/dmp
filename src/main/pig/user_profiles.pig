/*
Required parameters:
- date          target start date
- count         time interval in days
- input         path to input file or directory
- hosts         URIS to couchbase servers
- bucket        couchbase bucket
- password      couchbase password (if required)
- expiration    couchbase record expiration (if required)
- udf_file      path to pig-udfs.jar
*/

%default password ''
%default expiration 604800 
%default date `date +%Y-%m-%d`

REGISTER $udf_file

DEFINE DateStorage com.b5m.pig.udf.DateStorage('$date', '$count');
DEFINE Normalize com.b5m.pig.udf.NormalizeMap();
DEFINE Merge com.b5m.pig.udf.MergeMaps();
DEFINE CouchbaseStorage com.b5m.pig.udf.CouchbaseStorage('$hosts', '$bucket', '$password', '$expiration');

daily = LOAD '$input' USING DateStorage() AS (uuid:chararray, 
		page_category_count:[int], 
		product_category_count:[int], 
		price_count:[int],
        source_count:[int]);
		
grouped = GROUP daily BY uuid;

analytics = FOREACH grouped {
                page_merged = Merge(daily.page_category_count);
                page_normalized = Normalize(page_merged);
                product_merged = Merge(daily.product_category_count);
                product_normalized = Normalize(product_merged);
                price_merged = Merge(daily.price_count);
                price_normalized = Normalize(price_merged);
                source_merged = Merge(daily.source_count);
                source_normalized = Normalize(source_merged);
                GENERATE group AS uuid, 
                	page_normalized AS page_categories,
                	product_normalized AS product_categories,
                	price_normalized AS product_price,
                    source_normalized AS product_source;
            }
            
documents = FOREACH analytics GENERATE
                uuid AS key,
                TOTUPLE(uuid, '$date', $count, page_categories, 
                        product_categories, product_price, product_source)
                    AS value:(uuid:chararray, date:chararray, period:int, 
                        page_categories:[double], 
                        product_categories:[double], 
                        product_price:[double], 
                        product_source:[double]);
STORE documents INTO 'unused' USING CouchbaseStorage();

-- vim:ft=pig:nospell:
